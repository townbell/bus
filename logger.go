package bus

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

// LogLevel represents the severity level of a log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger defines the interface for logging within the event bus
type Logger interface {
	// Debug logs a debug message
	Debug(msg string, args ...interface{})
	// Info logs an info message
	Info(msg string, args ...interface{})
	// Warn logs a warning message
	Warn(msg string, args ...interface{})
	// Error logs an error message
	Error(msg string, args ...interface{})
	// SetLevel sets the minimum log level
	SetLevel(level LogLevel)
	// GetLevel returns the current log level
	GetLevel() LogLevel
}

// DefaultLogger is the default logger implementation using Go's standard log package
type DefaultLogger struct {
	level  atomic.Int32
	logger *log.Logger
}

// NewDefaultLogger creates a new default logger instance
func NewDefaultLogger() *DefaultLogger {
	logger := &DefaultLogger{
		logger: log.New(os.Stdout, "[EventBus] ", log.LstdFlags|log.Lshortfile),
	}
	logger.level.Store(int32(LogLevelInfo))
	return logger
}

// NewDefaultLoggerWithOutput creates a new default logger with custom output
func NewDefaultLoggerWithOutput(output *os.File, prefix string) *DefaultLogger {
	logger := &DefaultLogger{
		logger: log.New(output, prefix, log.LstdFlags|log.Lshortfile),
	}
	logger.level.Store(int32(LogLevelInfo))
	return logger
}

// Debug logs a debug message
func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	if LogLevel(l.level.Load()) <= LogLevelDebug {
		l.logger.Printf("[%s] %s", LogLevelDebug.String(), fmt.Sprintf(msg, args...))
	}
}

// Info logs an info message
func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	if LogLevel(l.level.Load()) <= LogLevelInfo {
		l.logger.Printf("[%s] %s", LogLevelInfo.String(), fmt.Sprintf(msg, args...))
	}
}

// Warn logs a warning message
func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	if LogLevel(l.level.Load()) <= LogLevelWarn {
		l.logger.Printf("[%s] %s", LogLevelWarn.String(), fmt.Sprintf(msg, args...))
	}
}

// Error logs an error message
func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	if LogLevel(l.level.Load()) <= LogLevelError {
		l.logger.Printf("[%s] %s", LogLevelError.String(), fmt.Sprintf(msg, args...))
	}
}

// SetLevel sets the minimum log level
func (l *DefaultLogger) SetLevel(level LogLevel) {
	l.level.Store(int32(level))
}

// GetLevel returns the current log level
func (l *DefaultLogger) GetLevel() LogLevel {
	return LogLevel(l.level.Load())
}

// NoOpLogger is a logger that does nothing (useful for disabling logging)
type NoOpLogger struct{}

// NewNoOpLogger creates a new no-op logger
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

// Debug does nothing
func (l *NoOpLogger) Debug(msg string, args ...interface{}) {}

// Info does nothing
func (l *NoOpLogger) Info(msg string, args ...interface{}) {}

// Warn does nothing
func (l *NoOpLogger) Warn(msg string, args ...interface{}) {}

// Error does nothing
func (l *NoOpLogger) Error(msg string, args ...interface{}) {}

// SetLevel does nothing
func (l *NoOpLogger) SetLevel(level LogLevel) {}

// GetLevel returns LogLevelError (highest level to disable all logging)
func (l *NoOpLogger) GetLevel() LogLevel {
	return LogLevelError + 1
}
