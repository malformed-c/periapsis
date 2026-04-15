package types

import "github.com/fogfish/golem/pure"

type FactKind any

// Fact is a happened event
// It is a read-only state change
type Fact[A any] pure.HKT[FactKind, A]
