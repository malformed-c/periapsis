package types

// Fact is a happened event.
// It is a read-only snapshot of a state change
type Fact interface {
	// isFact prevents external packages from implementing the Fact interface
	isFact()
}
