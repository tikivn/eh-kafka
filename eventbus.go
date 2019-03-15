package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/pkg/errors"
	"github.com/giautm/eh-kafka/json"
)

type KeyValueLogger interface {
	Log(keyVals ...interface{}) error
}

var Logger KeyValueLogger = &nopLogger{}

var ErrHandlerNil = errors.New("handler can't be nil")
var ErrMatcherNil = errors.New("matcher can't be nil")
var ErrTimedOut = errors.New("cannot run handle: timeout")

type TopicProducer func(event eh.Event) string

type TopicsConsumer func(event eh.EventHandler) []string

// EventBus is an event bus that notifies registered EventHandlers of
// published events. It will use the SimpleEventHandlingStrategy by default.
type EventBus struct {
	cancelFn           context.CancelFunc
	client             KafkaClient
	consumerCtx        context.Context
	consumerTopicsFunc TopicsConsumer
	encoder            Encoder
	producer           KafkaProducer
	producerTopicFunc  TopicProducer
	timeout            time.Duration
	waitConsumer       bool

	registered   map[eh.EventHandlerType]none
	registeredMu sync.RWMutex
	waitGroup    sync.WaitGroup

	errors    chan eh.EventBusError
	closed    chan none
	closeOnce sync.Once
}

// NewEventBus creates a EventBus.
func NewEventBus(
	ctx context.Context,
	brokers []string,
	producerTopicFunc TopicProducer,
	consumerTopicsFunc TopicsConsumer,
	opts ...Option,
) (*EventBus, error) {
	client := NewClient(brokers)
	return NewEventBusWithConfig(ctx, client, producerTopicFunc, consumerTopicsFunc, opts...)
}

// NewEventBus creates a EventBus.
func NewEventBusWithConfig(
	ctx context.Context,
	client KafkaClient,
	producerTopicFunc TopicProducer,
	consumerTopicsFunc TopicsConsumer,
	opts ...Option,
) (*EventBus, error) {
	options := Options{
		Encoder: json.NewEncoder(),
		Timeout: time.Second * 90,
	}
	for _, o := range opts {
		o(&options)
	}

	producer, err := client.NewProducer()
	if err != nil {
		return nil, err
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	return &EventBus{
		cancelFn:           cancel,
		client:             client,
		consumerCtx:        consumerCtx,
		consumerTopicsFunc: consumerTopicsFunc,
		encoder:            options.Encoder,
		producer:           producer,
		producerTopicFunc:  producerTopicFunc,
		registered:         map[eh.EventHandlerType]none{},
		timeout:            options.Timeout,
		waitConsumer:       false,

		errors: make(chan eh.EventBusError, 100),
		closed: make(chan none),
	}, nil
}

func (b *EventBus) SetWaitConsumer(wait bool) {
	b.waitConsumer = wait
}

func (b *EventBus) Close() (err error) {
	b.closeOnce.Do(func() {
		close(b.closed)
		close(b.errors)
		b.cancelFn()

		err = b.producer.Close()

		b.waitGroup.Wait()
		Logger.Log("msg", "All consumers exited")
	})

	return err
}

// PublishEvent publishes an event to all handlers capable of handling it.
func (b *EventBus) PublishEvent(ctx context.Context, event eh.Event) error {
	data, err := b.encoder.Encode(ctx, event)
	if err != nil {
		return err
	}

	topic := b.producerTopicFunc(event)
	key := ([]byte)(event.AggregateID().String())
	_, _, err = b.producer.SendMessage(ctx, topic, key, data)
	if err != nil {
		return errors.Wrapf(err,
			"could not publish event (%s) (%s)", event, ctx)
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(m eh.EventMatcher, h eh.EventHandler) {
	if err := b.subscription(m, h, false); err != nil {
		panic(err)
	}
}

// AddObserver implements the AddObserver method of the eventhorizon.EventBus interface.
func (b *EventBus) AddObserver(m eh.EventMatcher, h eh.EventHandler) {
	if err := b.subscription(m, h, true); err != nil {
		panic(err)
	}
}

// Errors returns an error channel where async handling errors are sent.
func (b *EventBus) Errors() <-chan eh.EventBusError {
	return b.errors
}

// Checks the matcher and handler and gets the event subscription.
func (b *EventBus) subscription(m eh.EventMatcher, h eh.EventHandler, observer bool) error {
	if m == nil {
		return ErrMatcherNil
	}
	if h == nil {
		return ErrHandlerNil
	}

	hType := h.HandlerType()
	b.registeredMu.Lock()
	if _, ok := b.registered[hType]; ok {
		b.registeredMu.Unlock()
		return errors.Errorf("multiple registrations for %s", hType)
	} else {
		b.registered[hType] = none{}
		b.registeredMu.Unlock()
	}

	groupID := string(hType)
	if observer { // Generate unique ID for each observer.
		uid := uuid.New()
		groupID = fmt.Sprintf("%s-%s", groupID, uid)
	}

	topics := b.consumerTopicsFunc(h)
	return b.handle(b.consumerCtx, groupID, topics, b.handler(m, h))
}

func (b *EventBus) handle(ctx context.Context, groupID string, topics []string, handler HandlerFunc) error {
	c, err := b.client.NewConsumer(ctx, groupID, topics)
	if err != nil {
		return err
	}

	go func() {
		for err := range c.Errors() {
			b.handleError(err)
			Logger.Log("groupID", groupID, "err", err)
		}
		Logger.Log("groupID", groupID, "msg", "Handle error exited")
	}()

	// Start consumer
	go func() {
		b.waitGroup.Add(1)
		Logger.Log("groupID", groupID, "msg", "New consumer")
		defer func() {
			b.handleError(c.Close())
			b.waitGroup.Done()
			Logger.Log("groupID", groupID, "msg", "Consumer exited")
		}()

		for {
			select {
			case <-b.closed:
				return
			default:
			}

			err := c.Receive(ctx, handler)
			if err != nil {
				if err == context.Canceled {
					return
				}
				b.handleError(err)
			}
		}
	}()

	// (giautm) NOTE: Waiting for consumer started, helpful in unit test.
	if b.waitConsumer {
		if err = c.Wait(b.timeout); err != nil {
			b.handleError(c.Close())
		}
	}

	return err
}

func (b *EventBus) handleError(err error) {
	select {
	case <-b.closed:
		return
	default:
	}

	if err != nil {
		select {
		case b.errors <- eh.EventBusError{Err: err}:
		default:
		}
	}
}

func (b *EventBus) handler(m eh.EventMatcher, h eh.EventHandler) HandlerFunc {
	return func(_ context.Context, msg ConsumerMessage) error {
		Logger.Log("topic", msg.Topic(), "offset", msg.Offset(), "key", (string)(msg.Key()))
		event, ctx, err := b.encoder.Decode(msg.Value())
		if err != nil {
			return eh.EventBusError{
				Err: err,
				Ctx: ctx,
			}
		}

		if m(event) {
			// Notify all observers about the event.
			if err := h.HandleEvent(ctx, event); err != nil {
				return eh.EventBusError{
					Err:   errors.Wrapf(err, "could not handle event (%s)", h.HandlerType()),
					Ctx:   ctx,
					Event: event,
				}
			}
		}

		return nil
	}
}
