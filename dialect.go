package sqlbuilder

import "sync/atomic"

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

/*
New starts an SQL statement with an sql verb.
Use From, Select, InsertInto or DeleteFrom methods to create
an instance of an SQL statement builder for common statements.
*/
func (b Dialect) New(verb string, args ...interface{}) Statement {
	q := getStmt(b)
	q.addChunk(posSelect, verb, "", args, ", ")
	return q
}
