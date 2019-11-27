package sqlbuilder

import (
	"sync"

	"github.com/valyala/bytebufferpool"
)

var (
	stmtPool = sync.Pool{New: newStmt}
)

func getBuffer() *bytebufferpool.ByteBuffer {
	return bytebufferpool.Get()
}

func putBuffer(buf *bytebufferpool.ByteBuffer) {
	bytebufferpool.Put(buf)
}

func newStmt() interface{} {
	return &statement{
		parts: make([]statementPart, 0, 8),
	}
}

func getStmt(d Dialect) *statement {
	stmt := stmtPool.Get().(*statement)
	stmt.dialect = d
	stmt.buffer = getBuffer()
	return stmt
}

func reuseStmt(stmt *statement) {
	stmt.parts = stmt.parts[:0]
	if len(stmt.args) > 0 {
		for n := range stmt.args {
			stmt.args[n] = nil
		}
		stmt.args = stmt.args[:0]
	}
	if len(stmt.dest) > 0 {
		for n := range stmt.dest {
			stmt.dest[n] = nil
		}
		stmt.dest = stmt.dest[:0]
	}
	putBuffer(stmt.buffer)
	stmt.buffer = nil
	if stmt.sql != nil {
		putBuffer(stmt.sql)
	}
	stmt.sql = nil

	stmtPool.Put(stmt)
}

var (
	space            = []byte{' '}
	placeholder      = []byte{'?'}
	placeholderComma = []byte{'?', ','}
	joinOn           = []byte{' ', 'O', 'N', ' ', '('}
)

const (
	_        = iota
	posStart = 100 * iota
	posWith
	posInsert
	posInsertFields
	posValues
	posDelete
	posUpdate
	posSet
	posSelect
	posInto
	posFrom
	posWhere
	posGroupBy
	posHaving
	posUnion
	posOrderBy
	posLimit
	posOffset
	posReturning
	posEnd
)

// addPart adds a clause or expression to a statement.
func (stmt *statement) addPart(pos int, clause, expr string, args []interface{}, sep string) (index int) {
	// Remember the position
	stmt.position = pos

	argLen := len(args)
	bufLow := len(stmt.buffer.B)
	index = len(stmt.parts)
	argTail := 0

	addNew := true
	addClause := clause != ""

	// Find the position to insert a part to
loop:
	for i := index - 1; i >= 0; i-- {
		part := &stmt.parts[i]
		index = i
		switch {
		// See if an existing part can be extended
		case part.position == pos:
			// Do nothing if a clause is already there and no expressions are to be added
			if expr == "" {
				// See if arguments are to be updated
				if argLen > 0 {
					copy(stmt.args[len(stmt.args)-argTail-part.argLen:], args)
				}
				return i
			}
			// Write a separator
			if part.hasExpr {
				stmt.buffer.WriteString(sep)
			} else {
				stmt.buffer.WriteString(" ")
			}
			if part.bufHigh == bufLow {
				// Do not add a part
				addNew = false
				// UpdateUser the existing one
				stmt.buffer.WriteString(expr)
				part.argLen += argLen
				part.bufHigh = len(stmt.buffer.B)
				part.hasExpr = true
			} else {
				// Do not add a clause
				addClause = false
				index = i + 1
			}
			break loop
		// No existing chunks of this type
		case part.position < pos:
			index = i + 1
			break loop
		default:
			argTail += part.argLen
		}
	}

	if addNew {
		// Insert a new part
		if addClause {
			stmt.buffer.WriteString(clause)
			if expr != "" {
				stmt.buffer.WriteString(" ")
			}
		}
		stmt.buffer.WriteString(expr)

		if cap(stmt.parts) == len(stmt.parts) {
			chunks := make([]statementPart, len(stmt.parts), cap(stmt.parts)*2)
			copy(chunks, stmt.parts)
			stmt.parts = chunks
		}

		part := statementPart{
			position: pos,
			bufLow:   bufLow,
			bufHigh:  len(stmt.buffer.B),
			argLen:   argLen,
			hasExpr:  expr != "",
		}

		stmt.parts = append(stmt.parts, part)
		if index < len(stmt.parts)-1 {
			copy(stmt.parts[index+1:], stmt.parts[index:])
			stmt.parts[index] = part
		}
	}

	// Insert query arguments
	if argLen > 0 {
		stmt.args = insertAt(stmt.args, args, len(stmt.args)-argTail)
	}
	stmt.Invalidate()

	return index
}

// join adds a join clause to a SELECT statement
func (stmt *statement) join(joinType, table, on string) (index int) {
	buf := bytebufferpool.Get()
	buf.WriteString(joinType)
	buf.WriteString(table)
	buf.Write(joinOn)
	buf.WriteString(on)
	buf.WriteByte(')')

	index = stmt.addPart(posFrom, "", bufferToString(&buf.B), nil, " ")

	bytebufferpool.Put(buf)

	return index
}
