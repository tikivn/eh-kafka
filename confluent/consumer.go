package confluent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	kafka2 "github.com/tikivn/eh-kafka"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type confluentKafkaConsumer struct {
	ctx         context.Context
	topics      []string
	csm         *kafka.Consumer
	chanErr     chan error
	sigTrans    chan os.Signal
	startedOnce sync.Once
	started     chan none
}

func newConsumer(ctx context.Context, cfg *kafka.ConfigMap, topics []string) (*confluentKafkaConsumer, error) {
	if len(topics) == 0 {
		return &confluentKafkaConsumer{}, errors.New("Topic to init consumer is empty")
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return &confluentKafkaConsumer{}, err
	}

	sigTrans := make(chan os.Signal, 1)
	signal.Notify(sigTrans, syscall.SIGINT, syscall.SIGTERM)

	return &confluentKafkaConsumer{
		csm:      c,
		chanErr:  make(chan error),
		sigTrans: sigTrans,
		ctx:      ctx,
		started:  make(chan none, 0),
	}, nil
}

func (c confluentKafkaConsumer) Errors() <-chan error {
	return c.chanErr
}

func (c confluentKafkaConsumer) Receive(ctx context.Context, handle kafka2.HandlerFunc) error {
	c.startedOnce.Do(func() {
		close(c.started)
	})
	return c.handleMessage(ctx, handle)
}

func (c confluentKafkaConsumer) Wait(timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return kafka.NewError(kafka.ErrTimedOut, "Consumer waiting reach timeout after %s ", true)
	case <-c.ctx.Done():
	case <-c.started:
	}

	return nil
}

func (c confluentKafkaConsumer) Close() error {
	err := c.csm.Unsubscribe()
	if err != nil {
		return err
	}

	return c.csm.Close()
}

func rebalanced(c *kafka.Consumer, e kafka.Event) error {
	logrus.Infof("Consumer %+v start rebalance on event %s", c.GetConsumerGroupMetadata(), e.String())
	return nil
}

func (c confluentKafkaConsumer) handleMessage(ctx context.Context, handlerFunc kafka2.HandlerFunc) error {
	err := c.csm.SubscribeTopics(c.topics, rebalanced)
	if err != nil {
		return err
	}

	run := true
	for run {
		select {
		case sig := <-c.sigTrans:
			logrus.Infof("Get signal %v, terminate consumer...", sig)
			run = false

		case ev := <-c.csm.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.csm.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.csm.Unassign()
			case *kafka.Message:
				err := handlerFunc(c.ctx, &msgWrap{mess: e})
				if err != nil {
					return err
				}

				_, err = c.csm.CommitMessage(e)
				if err != nil {
					return err
				}

			case kafka.PartitionEOF:
				fmt.Printf("Reached %v\n", e)
			case kafka.Error:
				c.chanErr <- e
			}
		}
	}
	return nil
}
