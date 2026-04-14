package types

type SpecFact struct {
	UID       string
	Namespace string
	PodName   string
}

func (SpecFact) isFact()
