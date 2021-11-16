package utils

import "testing"

func TestAtomicUint64_Get(t *testing.T) {
	var i AtomicUint64
	t.Log(i.Get())

	i.Set(10)

	t.Log(i.Get())
}
