package confluent

import (
	"context"
	"time"

	"github.com/pkg/errors"
	kafka2 "github.com/tikivn/eh-kafka"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type confluentKafkaProducer struct {
	prd          *kafka.Producer
	deliveryChan chan kafka.Event
}

func NewConfluentKafkaProducer(cfg *kafka.ConfigMap) (kafka2.KafkaProducer, error) {
	deliveryChan := make(chan kafka.Event)

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot create kafka producer with config %+v", cfg)
	}

	return &confluentKafkaProducer{
		prd:          producer,
		deliveryChan: deliveryChan,
	}, nil
}

func (p confluentKafkaProducer) SendMessage(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:         value,
		Key:           key,
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
	}

	err := p.prd.Produce(message, p.deliveryChan)
	if err != nil {
		return 0, 0, err
	}

	e := <-p.prd.Events()
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return 0, 0, errors.Wrapf(m.TopicPartition.Error, "Delivery failed")
	}

	return m.TopicPartition.Partition, int64(m.TopicPartition.Offset), nil

}

func (p confluentKafkaProducer) Close() error {
	p.prd.Close()
	return nil
}
