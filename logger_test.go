package bus

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLogger is a test logger that captures log messages
type TestLogger struct {
	buffer *bytes.Buffer
	level  LogLevel
	mu     sync.Mutex
}

func NewTestLogger() *TestLogger {
	return &TestLogger{
		buffer: &bytes.Buffer{},
		level:  LogLevelDebug,
	}
}

func (l *TestLogger) Debug(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= LogLevelDebug {
		formatted := fmt.Sprintf(msg, args...)
		l.buffer.WriteString("[DEBUG] " + formatted + "\n")
	}
}

func (l *TestLogger) Info(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= LogLevelInfo {
		formatted := fmt.Sprintf(msg, args...)
		l.buffer.WriteString("[INFO] " + formatted + "\n")
	}
}

func (l *TestLogger) Warn(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= LogLevelWarn {
		formatted := fmt.Sprintf(msg, args...)
		l.buffer.WriteString("[WARN] " + formatted + "\n")
	}
}

func (l *TestLogger) Error(msg string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level <= LogLevelError {
		formatted := fmt.Sprintf(msg, args...)
		l.buffer.WriteString("[ERROR] " + formatted + "\n")
	}
}

func (l *TestLogger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *TestLogger) GetLevel() LogLevel {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

func (l *TestLogger) GetLogs() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.String()
}

func (l *TestLogger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buffer.Reset()
}

func TestDefaultLogger(t *testing.T) {
	logger := NewDefaultLogger()

	// Test level setting
	logger.SetLevel(LogLevelWarn)
	if logger.GetLevel() != LogLevelWarn {
		t.Errorf("Expected log level %v, got %v", LogLevelWarn, logger.GetLevel())
	}

	// Test that debug and info are filtered out
	logger.Debug("This should not appear")
	logger.Info("This should not appear")
	logger.Warn("This should appear")
	logger.Error("This should appear")
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	// Should not panic
	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")
	logger.SetLevel(LogLevelDebug)

	// Should return a high level to disable all logging
	if logger.GetLevel() <= LogLevelError {
		t.Errorf("NoOpLogger should return a level higher than LogLevelError")
	}
}

func TestEventBusWithLogger(t *testing.T) {
	testLogger := NewTestLogger()
	eventBus := NewTyped[string]()
	eventBus.SetLogger(testLogger)

	// Test that logger is set correctly
	if eventBus.GetLogger() != testLogger {
		t.Error("Logger was not set correctly")
	}

	// Test subscription logging
	handle := mustSubscribe(t, eventBus, "test.topic", discard[string])

	logs := testLogger.GetLogs()
	if !strings.Contains(logs, "Handler subscribed to topic 'test.topic'") {
		t.Errorf("Expected subscription log, got: %s", logs)
	}

	testLogger.Clear()

	// Test publishing logging
	eventBus.Publish("test.topic", "test data")

	logs = testLogger.GetLogs()
	if !strings.Contains(logs, "Publishing event to topic 'test.topic'") {
		t.Errorf("Expected publishing log, got: %s", logs)
	}
	if !strings.Contains(logs, "Handler executed successfully for topic 'test.topic'") {
		t.Errorf("Expected handler execution log, got: %s", logs)
	}

	testLogger.Clear()

	// Test unsubscription logging
	handle.Unsubscribe()

	logs = testLogger.GetLogs()
	if !strings.Contains(logs, "Unsubscribing handler from topic 'test.topic'") {
		t.Errorf("Expected unsubscription log, got: %s", logs)
	}
	if !strings.Contains(logs, "Handler removed from topic 'test.topic'") {
		t.Errorf("Expected handler removal log, got: %s", logs)
	}
}

func TestEventBusLoggerWithPanic(t *testing.T) {
	testLogger := NewTestLogger()
	eventBus := NewTyped[string]()
	eventBus.SetLogger(testLogger)

	// Subscribe a handler that panics
	mustSubscribe(t, eventBus, "panic.topic", func(ctx context.Context, data string) error {
		panic("test panic")
	})

	// Publish event
	eventBus.Publish("panic.topic", "test data")

	logs := testLogger.GetLogs()
	if !strings.Contains(logs, "Handler panic for topic 'panic.topic'") {
		t.Errorf("Expected panic log, got: %s", logs)
	}
}

func TestEventBusLoggerWithHandlerError(t *testing.T) {
	testLogger := NewTestLogger()
	eventBus := NewTyped[string]()
	eventBus.SetLogger(testLogger)

	mustSubscribe(t, eventBus, "error.topic", func(ctx context.Context, data string) error {
		return fmt.Errorf("business failure")
	})

	_ = eventBus.Publish("error.topic", "test data")

	logs := testLogger.GetLogs()
	if !strings.Contains(logs, "Handler failed for topic 'error.topic'") {
		t.Errorf("Expected handler failure log, got: %s", logs)
	}
	if strings.Contains(logs, "Handler panic") {
		t.Errorf("A returned error must not be logged as a panic, got: %s", logs)
	}
}

func TestEventBusLoggerLevels(t *testing.T) {
	testLogger := NewTestLogger()
	eventBus := NewTyped[string]()
	eventBus.SetLogger(testLogger)

	// Set logger to only show errors
	testLogger.SetLevel(LogLevelError)

	// Subscribe and publish
	mustSubscribe(t, eventBus, "test.topic", discard[string])
	eventBus.Publish("test.topic", "test data")

	logs := testLogger.GetLogs()
	// Should not contain debug messages
	if strings.Contains(logs, "[DEBUG]") {
		t.Errorf("Debug messages should be filtered out, got: %s", logs)
	}

	// Test with panic (should show error)
	testLogger.Clear()
	mustSubscribe(t, eventBus, "panic.topic", func(ctx context.Context, data string) error {
		panic("test panic")
	})
	eventBus.Publish("panic.topic", "test data")

	logs = testLogger.GetLogs()
	if !strings.Contains(logs, "[ERROR]") {
		t.Errorf("Expected error log for panic, got: %s", logs)
	}
}

func TestEventBusWithNoOpLogger(t *testing.T) {
	eventBus := NewTyped[string]()
	eventBus.SetLogger(NewNoOpLogger())

	// Should not panic with no-op logger
	mustSubscribe(t, eventBus, "test.topic", discard[string])
	eventBus.Publish("test.topic", "test data")

	// Subscribe a handler that panics
	mustSubscribe(t, eventBus, "panic.topic", func(ctx context.Context, data string) error {
		panic("test panic")
	})
	eventBus.Publish("panic.topic", "test data")
}

func TestLoggerConcurrency(t *testing.T) {
	testLogger := NewTestLogger()
	eventBus := NewTyped[int]()
	eventBus.SetLogger(testLogger)

	// Test concurrent access to logger
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			topic := "concurrent.topic"
			_, _ = eventBus.Subscribe(topic, func(ctx context.Context, data int) error {
				time.Sleep(1 * time.Millisecond)
				return nil
			})

			for j := 0; j < 5; j++ {
				eventBus.Publish(topic, id*10+j)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and should have logs
	logs := testLogger.GetLogs()
	if len(logs) == 0 {
		t.Error("Expected some logs from concurrent operations")
	}
}

func TestDefaultLoggerLevelConcurrency(t *testing.T) {
	logger := NewDefaultLogger()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				logger.SetLevel(LogLevel((i + j) % 4))
				_ = logger.GetLevel()
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkEventBusWithLogger(b *testing.B) {
	eventBus := NewTyped[string]()
	eventBus.SetLogger(NewDefaultLogger())

	mustSubscribe(b, eventBus, "bench.topic", discard[string])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eventBus.Publish("bench.topic", "benchmark data")
	}
}

func BenchmarkEventBusWithNoOpLogger(b *testing.B) {
	eventBus := NewTyped[string]()
	eventBus.SetLogger(NewNoOpLogger())

	mustSubscribe(b, eventBus, "bench.topic", discard[string])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eventBus.Publish("bench.topic", "benchmark data")
	}
}

// TestNewDefaultLoggerWithOutput tests the NewDefaultLoggerWithOutput function
func TestNewDefaultLoggerWithOutput(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "logger_test_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	logger := NewDefaultLoggerWithOutput(tmpFile, "[TEST] ")

	// Test that logger is created successfully
	if logger == nil {
		t.Error("Expected logger to be created, got nil")
	}

	// Test level setting
	logger.SetLevel(LogLevelError)
	if logger.GetLevel() != LogLevelError {
		t.Errorf("Expected log level %v, got %v", LogLevelError, logger.GetLevel())
	}

	// Test logging to file
	logger.Error("error message")
	tmpFile.Sync() // Ensure data is written to file

	// Read file content
	tmpFile.Seek(0, 0)
	content := make([]byte, 1024)
	n, _ := tmpFile.Read(content)
	output := string(content[:n])

	if !strings.Contains(output, "error message") {
		t.Errorf("Expected output to contain 'error message', got: %s", output)
	}
	if !strings.Contains(output, "[TEST]") {
		t.Errorf("Expected output to contain '[TEST]' prefix, got: %s", output)
	}
}

// TestNoOpLoggerAllMethods tests all methods of NoOpLogger
func TestNoOpLoggerAllMethods(t *testing.T) {
	logger := NewNoOpLogger()

	// Test that all methods can be called without panic
	logger.Debug("debug test")
	logger.Info("info test")
	logger.Warn("warn test")
	logger.Error("error test")

	// Test SetLevel (should not panic)
	logger.SetLevel(LogLevelDebug)
	logger.SetLevel(LogLevelInfo)
	logger.SetLevel(LogLevelWarn)
	logger.SetLevel(LogLevelError)

	// Test GetLevel returns a high value
	level := logger.GetLevel()
	if level <= LogLevelError {
		t.Errorf("NoOpLogger should return a level higher than LogLevelError, got %v", level)
	}
}

// TestLogLevelString tests the String method of LogLevel
func TestLogLevelString(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{LogLevelDebug, "DEBUG"},
		{LogLevelInfo, "INFO"},
		{LogLevelWarn, "WARN"},
		{LogLevelError, "ERROR"},
		{LogLevel(999), "UNKNOWN"},
	}

	for _, test := range tests {
		result := test.level.String()
		if result != test.expected {
			t.Errorf("Expected %s for level %v, got %s", test.expected, test.level, result)
		}
	}
}

// TestDefaultLoggerDebugAndInfo tests Debug and Info methods of DefaultLogger
func TestDefaultLoggerDebugAndInfo(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "logger_debug_test_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	logger := NewDefaultLoggerWithOutput(tmpFile, "[DEBUG_TEST] ")

	// Set to debug level to capture all messages
	logger.SetLevel(LogLevelDebug)

	// Test Debug method
	logger.Debug("debug message with %s", "formatting")
	tmpFile.Sync()

	// Read file content
	tmpFile.Seek(0, 0)
	content := make([]byte, 1024)
	n, _ := tmpFile.Read(content)
	output := string(content[:n])

	if !strings.Contains(output, "debug message with formatting") {
		t.Errorf("Expected debug message in output, got: %s", output)
	}

	// Test Info method - truncate file and test again
	tmpFile.Truncate(0)
	tmpFile.Seek(0, 0)
	logger.Info("info message with %d", 42)
	tmpFile.Sync()

	tmpFile.Seek(0, 0)
	n, _ = tmpFile.Read(content)
	output = string(content[:n])

	if !strings.Contains(output, "info message with 42") {
		t.Errorf("Expected info message in output, got: %s", output)
	}

	// Test that debug and info are filtered when level is higher
	tmpFile.Truncate(0)
	tmpFile.Seek(0, 0)
	logger.SetLevel(LogLevelWarn)

	logger.Debug("filtered debug")
	logger.Info("filtered info")
	tmpFile.Sync()

	tmpFile.Seek(0, 0)
	n, _ = tmpFile.Read(content)
	output = string(content[:n])

	if strings.Contains(output, "filtered debug") || strings.Contains(output, "filtered info") {
		t.Errorf("Debug and Info messages should be filtered out, got: %s", output)
	}
}
