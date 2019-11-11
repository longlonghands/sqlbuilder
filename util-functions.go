package sqlbuilder

import (
	"unsafe"
)

func insertAt(dest, src []interface{}, index int) []interface{} {
	srcLen := len(src)
	if srcLen > 0 {
		oldLen := len(dest)
		newLen := oldLen + srcLen
		// Allocate more memory if needed
		if cap(dest) < newLen {
			newCap := cap(dest) * 2
			if newCap == 0 {
				newCap = 10
			}
			qargs := make([]interface{}, oldLen, newCap)
			copy(qargs, dest)
			dest = qargs
		}
		dest = append(dest, src...)
		if index < oldLen {
			copy(dest[index+srcLen:], dest[index:])
			copy(dest[index:], src)
		}
	}

	return dest
}

// bufferToString returns a string pointing to a ByteBuffer contents
// It helps to avoid memory copyng.
// Care:
// Use the returned string with care, make sure to never use it after
// the ByteBuffer is deallocated or returned to a pool.
func bufferToString(buffer *[]byte) string {
	return *(*string)(unsafe.Pointer(buffer))
}
