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
