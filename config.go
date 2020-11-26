package kafka

import (
	"time"
)

type Options struct {
	Encoder Encoder
	Timeout time.Duration
}

type Option func(o *Options)

func WithEncoder(encoder Encoder) Option {
	return func(o *Options) {
		o.Encoder = encoder
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.Timeout = timeout
	}
}

type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

type Config struct {
	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes.
	ClientID string
	Acks     RequiredAcks
	Retry    struct {
		Max int32
		// How long to wait for the cluster to settle between retries
		// (default 100ms). Similar to the `retry.backoff.ms` setting
		Backoff time.Duration
	}
	Flush struct {
		// The best-effort number of bytes needed to trigger a flush. Use the
		// global sarama.MaxRequestSize to set a hard upper limit.
		Bytes int
		// The best-effort number of messages needed to trigger a flush. Use
		// `MaxMessages` to set a hard upper limit.
		Messages int
		// The best-effort frequency of flushes. Equivalent to
		// `queue.buffering.max.ms` setting of JVM producer.
		Frequency time.Duration
		// The maximum number of messages the producer will send in a single
		// broker request. Defaults to 0 for unlimited. Similar to
		// `queue.buffering.max.messages` in the JVM producer.
		MaxMessages int
	}
}

func DefaultConfig() Config {
	cfg := Config{}

	cfg.ClientID = "eh-kafka-eventbus"
	cfg.Acks = 1

	cfg.Retry.Max = 10
	cfg.Retry.Backoff = time.Second
	return cfg
}
