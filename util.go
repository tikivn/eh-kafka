package kafka

type nopLogger struct{}

func (*nopLogger) Log(_ ...interface{}) error {
	return nil
}

type none struct{}
