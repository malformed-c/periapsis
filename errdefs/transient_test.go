package errdefs

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

type testingTransientError bool

func (e testingTransientError) Error() string {
	return fmt.Sprintf("%v", bool(e))
}

func (e testingTransientError) Transient() bool {
	return bool(e)
}

func TestIsTransient(t *testing.T) {
	type testCase struct {
		name       string
		err        error
		xMsg       string
		xTransient bool
	}

	for _, c := range []testCase{
		{
			name:       "Transientf",
			err:        Transientf("%s is pending", "PVC"),
			xMsg:       "PVC is pending",
			xTransient: true,
		},
		{
			name:       "AsTransient",
			err:        AsTransient(errors.New("this is a test")),
			xMsg:       "this is a test",
			xTransient: true,
		},
		{
			name:       "AsTransientWithNil",
			err:        AsTransient(nil),
			xMsg:       "",
			xTransient: false,
		},
		{
			name:       "nilError",
			err:        nil,
			xMsg:       "",
			xTransient: false,
		},
		{
			name:       "customTransientFalse",
			err:        testingTransientError(false),
			xMsg:       "false",
			xTransient: false,
		},
		{
			name:       "customTransientTrue",
			err:        testingTransientError(true),
			xMsg:       "true",
			xTransient: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			assert.Check(t, cmp.Equal(IsTransient(c.err), c.xTransient))
			if c.err != nil {
				assert.Check(t, cmp.Equal(c.err.Error(), c.xMsg))
			}
		})
	}
}

func TestTransientCause(t *testing.T) {
	err := errors.New("test")
	e := &transientError{err}
	assert.Check(t, cmp.Equal(e.Cause(), err))
	assert.Check(t, IsTransient(errors.Wrap(e, "some details")))
}
