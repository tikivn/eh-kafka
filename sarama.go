package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func NewClient(brokers []string) KafkaClient {
	return NewClientWithConfig(brokers, NewConfig())
}

func NewConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "eh-kafka-eventbus"
	cfg.Version = sarama.V2_0_0_0

	cfg.Consumer.Return.Errors = true
	cfg.Producer.Flush.MaxMessages = 1
	cfg.Producer.Return.Successes = true

	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = time.Second

	cfg.Producer.RequiredAcks = sarama.WaitForAll

	return cfg
}

func NewConfigWithBatchProducer() *sarama.Config {
	cfg := NewConfig()

	cfg.Producer.Flush.Messages = 1000
	cfg.Producer.Flush.MaxMessages = 10000
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond

	return cfg
}

type kafkaClient struct {
	brokers []string
	config  *sarama.Config
}

func NewClientWithConfig(brokers []string, cfg *sarama.Config) KafkaClient {
	return &kafkaClient{
		brokers: brokers,
		config:  cfg,
	}
}

func (c *kafkaClient) NewProducer() (KafkaProducer, error) {
	p, err := sarama.NewSyncProducer(c.brokers, c.config)
	if err != nil {
		return nil, err
	}

	return NewSyncProducer(p)
}

func (c *kafkaClient) NewConsumer(ctx context.Context, id string, topics []string) (KafkaConsumer, error) {
	group, err := sarama.NewConsumerGroup(c.brokers, id, c.config)
	if err != nil {
		return nil, err
	}

	return NewConsumer(ctx, group, topics)
}

type producer struct {
	producer sarama.SyncProducer
}

func NewSyncProducer(p sarama.SyncProducer) (KafkaProducer, error) {
	return &producer{
		producer: p,
	}, nil
}

func (p *producer) SendMessage(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	return p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
}

func (p *producer) Close() error {
	p.producer.Close()
	return nil
}

type consumer struct {
	ctx         context.Context
	group       sarama.ConsumerGroup
	topics      []string
	started     chan none
	startedOnce sync.Once
}

func NewConsumer(ctx context.Context, group sarama.ConsumerGroup, topics []string) (KafkaConsumer, error) {
	return &consumer{
		ctx:     ctx,
		group:   group,
		topics:  topics,
		started: make(chan none, 0),
	}, nil
}

func (c *consumer) Receive(ctx context.Context, f HandlerFunc) error {
	handler := &consumerGroupHandler{
		handler: f,
		started: func() {
			c.startedOnce.Do(func() {
				close(c.started)
			})
		},
	}

	err := c.group.Consume(ctx, c.topics, handler)
	return err
}

func (c *consumer) Wait(timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return ErrTimedOut
	case <-c.ctx.Done():
	case <-c.started:
	}

	return nil
}

func (c *consumer) Errors() <-chan error {
	return c.group.Errors()
}

func (c *consumer) Close() error {
	return c.group.Close()
}

type consumerGroupHandler struct {
	handler HandlerFunc
	started func()
}

func (c *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	c.started()
	return nil
}

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroupHandler) ConsumeClaim(
	s sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		err := c.handler(s.Context(), &msgWrap{msg})
		if err != nil {
			return err
		}
		s.MarkMessage(msg, "")
	}

	return nil
}

type msgWrap struct {
	*sarama.ConsumerMessage
}

func (m *msgWrap) Topic() string {
	return m.ConsumerMessage.Topic
}

func (m *msgWrap) Offset() int64 {
	return m.ConsumerMessage.Offset
}

func (m *msgWrap) Key() []byte {
	return m.ConsumerMessage.Key
}

func (m *msgWrap) Value() []byte {
	return m.ConsumerMessage.Value
}
