package helper

import (
	"iter"
)

// Map transforms an iterator of T into an iterator of U.
// It is "lazy": no work is done until the returned iterator is consumed.
func Map[T, U any](seq iter.Seq[T], f func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for v := range seq {
			if !yield(f(v)) {
				return
			}
		}
	}
}
