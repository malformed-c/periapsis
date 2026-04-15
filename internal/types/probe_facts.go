package types

// ProbeFact is emitted when a probe (readiness, liveness, startup) completes.
// The Focus uses this to update the container's ready condition.
type ProbeFact struct {
	UID       string
	Container string

	// "readiness", "liveness", or "startup"
	ProbeType string

	// Whether the probe succeeded.
	Success bool

	// Thresholds from the probe spec (default 1 for success, 3 for failure).
	SuccessThreshold int32
	FailureThreshold int32
}

func (ProbeFact) isFact()
