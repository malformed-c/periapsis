package errdefs

import (
	"errors"
	"fmt"
)

// ErrTransient is an error interface which denotes whether the operation failed due
// to a transient condition that should be retried.
type ErrTransient interface {
	Transient() bool
	error
}

type transientError struct {
	error
}

func (e *transientError) Transient() bool {
	return true
}

func (e *transientError) Cause() error {
	return e.error
}

// AsTransient wraps the passed in error to make it of type ErrTransient
//
// Callers should make sure the passed in error has exactly the error message
// it wants as this function does not decorate the message.
func AsTransient(err error) error {
	if err == nil {
		return nil
	}
	return &transientError{err}
}

// Transient makes an ErrTransient from the provided error message
func Transient(msg string) error {
	return &transientError{errors.New(msg)}
}

// Transientf makes an ErrTransient from the provided error format and args
func Transientf(format string, args ...any) error {
	return &transientError{fmt.Errorf(format, args...)}
}

// IsTransient determines if the passed in error is of type ErrTransient
//
// This will traverse the causal chain (`Cause() error`), until it finds an error
// which implements the `Transient` interface.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(ErrTransient); ok {
		return e.Transient()
	}

	if e, ok := err.(causal); ok {
		return IsTransient(e.Cause())
	}

	return false
}
