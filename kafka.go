package kafka

import (
	"context"
	"time"
)

type KafkaClient interface {
	NewProducer() (KafkaProducer, error)
	NewConsumer(_ context.Context, id string, topics []string) (KafkaConsumer, error)
}

type KafkaProducer interface {
	SendMessage(ctx context.Context, topic string, key, value []byte) (int32, int64, error)
	Close() error
}

type ConsumerMessage interface {
	Topic() string
	Offset() int64
	Key() []byte
	Value() []byte
}

type HandlerFunc func(context.Context, ConsumerMessage) error

type KafkaConsumer interface {
	Errors() <-chan error
	Receive(context.Context, *consumerGroupHandler) error
	Wait(time.Duration) error
	Close() error
}
