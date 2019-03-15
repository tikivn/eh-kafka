package kafka_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/eventbus"
	"github.com/rcrowley/go-metrics"
	kafka "github.com/giautm/eh-kafka"
	"go.uber.org/goleak"
)

func init() {
	// Disable metrics, it cause goroutine leaks
	metrics.UseNilMetrics = true
}

func TestEventBus(t *testing.T) {
	defer goleak.VerifyNoLeaks(t)
	logger := kitlog.NewLogfmtLogger(os.Stdout)
	logger = kitlog.With(logger, "TestEventBus", os.Getegid())

	kfbus.Logger = logger
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topic := uuid.New()

	timeout := time.Second * 60

	ctx := context.Background()

	bus1, err := kfbus.NewEventBus(
		ctx,
		brokers,
		func(eh.Event) string { return topic.String() },
		func(eh.EventHandler) []string { return []string{topic.String()} },
		kfbus.WithTimeout(timeout),
	)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	bus1.SetWaitConsumer(true)
	defer bus1.Close()

	bus2, err := kfbus.NewEventBus(
		ctx,
		brokers,
		func(eh.Event) string { return topic.String() },
		func(eh.EventHandler) []string { return []string{topic.String()} },
		kfbus.WithTimeout(timeout),
	)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	bus2.SetWaitConsumer(true)
	defer bus2.Close()

	eventbus.AcceptanceTest(t, bus1, bus2, timeout)
}
