package kafka

import (
	"context"

	eh "github.com/looplab/eventhorizon"
)

// Encoder is an interface that allow eventbus encode/decode event
type Encoder interface {
	Decode([]byte) (eh.Event, context.Context, error)
	Encode(context.Context, eh.Event) ([]byte, error)
	String() string
}
