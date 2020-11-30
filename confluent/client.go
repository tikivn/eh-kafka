package confluent

import (
	"context"
	"strings"
	"time"

	kafka2 "github.com/tikivn/eh-kafka"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type confluentKafkaClient struct {
	cfg     *kafka.ConfigMap
	brokers []string
}

func (c confluentKafkaClient) NewProducer() (kafka2.KafkaProducer, error) {
	return NewConfluentKafkaProducer(c.cfg)
}

func (c confluentKafkaClient) NewConsumer(ctx context.Context, groupId string, topics []string) (kafka2.KafkaConsumer, error) {
	cfg := c.cfg
	cfg.SetKey("group_id", groupId)
	cfg.SetKey("bootstrap.servers", strings.Join(c.brokers, ","))

	return newConsumer(ctx, cfg, topics)
}

func NewClient(brokers []string) *confluentKafkaClient {
	defaultConfig := NewConfig()
	return &confluentKafkaClient{
		cfg:     defaultConfig,
		brokers: brokers,
	}
}

func NewConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"session.timeout.ms":       6000,
		"go.events.channel.enable": true,
		"client.id":                "eh-kafka-eventbus",
		"batch.size":               0,
		"retry.backoff.ms":         time.Second,
		"request.required.acks":    1,
	}
}

type msgWrap struct {
	mess *kafka.Message
}

func (m *msgWrap) Topic() string {
	if m.mess.TopicPartition.Topic != nil {
		return *(m.mess.TopicPartition.Topic)
	}
	return ""
}

func (m *msgWrap) Offset() int64 {
	return int64(m.mess.TopicPartition.Offset)
}

func (m *msgWrap) Key() []byte {
	return m.mess.Key
}

func (m *msgWrap) Value() []byte {
	return m.mess.Value
}

type none struct{}
