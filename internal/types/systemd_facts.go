package types

type UnitFact struct {
	UID      string
	UnitName string
	State    string // e.g., "running", "failed"
}

func (UnitFact) isFact()

type ExitFact struct {
	UID      string
	ExitCode int32
}

func (ExitFact) isFact()
