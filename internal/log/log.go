package log

type LeveledLogger interface {
	CheckDebug() bool
	CheckInfo() bool

	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
}

type noopLogger struct{}

func (noopLogger) CheckDebug() bool     { return false }
func (noopLogger) CheckInfo() bool      { return false }
func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}

var Default LeveledLogger = noopLogger{}
