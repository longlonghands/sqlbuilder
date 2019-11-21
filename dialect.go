package sqlbuilder

import (
	"strconv"
	"sync/atomic"

	"github.com/valyala/bytebufferpool"
)

// Dialect defines the method SQL statement is to be built.
type Dialect uint32

const (
	// DefaultDialect is a default statement builder mode.
	DefaultDialect Dialect = iota
	// PostgreSQL dialect is to be used to automatically replace ? placeholders with $1, $2...
	PostgreSQL
)

var selectedDialect = DefaultDialect

// SetDialect selects a Dialect to be used by default.
func SetDialect(dialect Dialect) {
	atomic.StoreUint32((*uint32)(&selectedDialect), uint32(dialect))
}

// writePostgresql function copies s into buf and replaces ? placeholders with $1, $2...
func writePostgresql(argNo int, s []byte, buf *bytebufferpool.ByteBuffer) (int, error) {
	var err error
	start := 0
	// Iterate by runes
	for pos, r := range bufferToString(&s) {
		if start > pos {
			continue
		}
		switch r {
		case '\\':
			if pos < len(s)-1 && s[pos+1] == '?' {
				_, err = buf.Write(s[start:pos])
				if err == nil {
					err = buf.WriteByte('?')
				}
				start = pos + 2
			}
		case '?':
			_, err = buf.Write(s[start:pos])
			start = pos + 1
			if err == nil {
				err = buf.WriteByte('$')
				if err == nil {
					buf.B = strconv.AppendInt(buf.B, int64(argNo), 10)
					argNo++
				}
			}
		}
		if err != nil {
			break
		}
	}
	if err == nil && start < len(s) {
		_, err = buf.Write(s[start:])
	}
	return argNo, err
}
