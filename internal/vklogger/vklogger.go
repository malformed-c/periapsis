// Package vklogger provides a periapsis log.Logger implementation
// that writes directly to a slog.Logger.
package vklogger

import (
	"fmt"
	"log/slog"
	"strings"

	vklog "github.com/malformed-c/periapsis/log"
)

var _ vklog.Logger = (*adapter)(nil)

// suppress contains substrings of VK messages to drop entirely.
// These are high-frequency operational noise with no diagnostic value.
var suppress = []string{
	"lease",
	"Lease",
	"Pod status update loop",
	"sync handled",
	"processing pod status update",
}

func isSuppressed(msg string) bool {
	for _, s := range suppress {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

type adapter struct {
	logger *slog.Logger
}

// New returns a vklog.Logger that writes to the given slog.Logger.
func New(l *slog.Logger) vklog.Logger {
	return &adapter{logger: l}
}

func (a *adapter) emit(level slog.Level, msg string) {
	if isSuppressed(msg) {
		return
	}
	a.logger.Log(nil, level, msg)
}

func (a *adapter) Debug(args ...interface{})                 { a.emit(slog.LevelDebug, fmt.Sprint(args...)) }
func (a *adapter) Debugf(format string, args ...interface{}) { a.emit(slog.LevelDebug, fmt.Sprintf(format, args...)) }
func (a *adapter) Info(args ...interface{})                  { a.emit(slog.LevelInfo, fmt.Sprint(args...)) }
func (a *adapter) Infof(format string, args ...interface{})  { a.emit(slog.LevelInfo, fmt.Sprintf(format, args...)) }
func (a *adapter) Warn(args ...interface{})                  { a.emit(slog.LevelWarn, fmt.Sprint(args...)) }
func (a *adapter) Warnf(format string, args ...interface{})  { a.emit(slog.LevelWarn, fmt.Sprintf(format, args...)) }
func (a *adapter) Error(args ...interface{})                 { a.emit(slog.LevelError, fmt.Sprint(args...)) }
func (a *adapter) Errorf(format string, args ...interface{}) { a.emit(slog.LevelError, fmt.Sprintf(format, args...)) }
func (a *adapter) Fatal(args ...interface{})                 { a.emit(slog.LevelError, fmt.Sprint(args...)) }
func (a *adapter) Fatalf(format string, args ...interface{}) { a.emit(slog.LevelError, fmt.Sprintf(format, args...)) }

func (a *adapter) WithField(key string, val interface{}) vklog.Logger {
	return &adapter{logger: a.logger.With(key, val)}
}
func (a *adapter) WithFields(fields vklog.Fields) vklog.Logger {
	args := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &adapter{logger: a.logger.With(args...)}
}
func (a *adapter) WithError(err error) vklog.Logger {
	return &adapter{logger: a.logger.With("err", err)}
}
