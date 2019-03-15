package json

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/pkg/errors"
)

func NewEncoder() *Encoder {
	return &Encoder{}
}

type Encoder struct{}

func (Encoder) String() string {
	return "json"
}

func (Encoder) Decode(rawData []byte) (eh.Event, context.Context, error) {
	var e evtJSON
	if err := json.Unmarshal(rawData, &e); err != nil {
		return nil, nil, errors.Wrap(err, "could not unmarshal event")
	}

	// Create an event of the correct type.
	if data, err := eh.CreateEventData(e.EventType); err == nil {
		// Manually decode the raw JSON event.
		if err := json.Unmarshal(e.RawData, data); err != nil {
			return nil, nil, errors.Wrap(err, "could not unmarshal event data")
		}

		// Set concrete event and zero out the decoded event.
		e.data = data
		e.RawData = nil
	}

	event := event{evtJSON: e}
	ctx := eh.UnmarshalContext(e.Context)
	return event, ctx, nil
}

func (Encoder) Encode(ctx context.Context, event eh.Event) ([]byte, error) {
	e := evtJSON{
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		rawData, err := json.Marshal(event.Data())
		if err != nil {
			return nil, errors.Wrapf(err,
				"could not marshal event data (%s), context (%s)", event, ctx)
		}
		e.RawData = json.RawMessage(rawData)
	}

	data, err := json.Marshal(e)
	if err != nil {
		return nil, errors.Wrapf(err,
			"could not marshal event (%s), context (%s)", event, ctx)
	}

	return data, nil
}

// evtJSON is the internal event used on the wire only.
type evtJSON struct {
	EventType     eh.EventType           `json:"event_type"`
	RawData       json.RawMessage        `json:"data,omitempty"`
	Timestamp     time.Time              `json:"timestamp"`
	AggregateType eh.AggregateType       `json:"aggregate_type"`
	AggregateID   uuid.UUID              `json:"_id"`
	Version       int                    `json:"version"`
	Context       map[string]interface{} `json:"context"`
	data          eh.EventData
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	evtJSON
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.evtJSON.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.evtJSON.data
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.evtJSON.Timestamp
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.evtJSON.AggregateType
}

// AggregateID implements the AggregateID method of the eventhorizon.Event interface.
func (e event) AggregateID() uuid.UUID {
	return e.evtJSON.AggregateID
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.evtJSON.Version
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.evtJSON.EventType, e.evtJSON.Version)
}
