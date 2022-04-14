package util

import "unsafe"

//go:linkname runtimeMemhash runtime.memhash
//go:noescape
func runtimeMemhash(p unsafe.Pointer, seed, s uintptr) uintptr

// MemHash is the hash function used by go map, it utilizes available hardware instructions(behaves
// as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHash(buf []byte) uint64 {
	return rthash(buf, 923)
}

func rthash(b []byte, seed uint64) uint64 {
	if len(b) == 0 {
		return seed
	}
	// The runtime hasher only works on uintptr. For 64-bit
	// architectures, we use the hasher directly. Otherwise,
	// we use two parallel hashers on the lower and upper 32 bits.
	if unsafe.Sizeof(uintptr(0)) == 8 {
		return uint64(runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b))))
	}
	lo := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b)))
	hi := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed>>32), uintptr(len(b)))
	return uint64(hi)<<32 | uint64(lo)
}
