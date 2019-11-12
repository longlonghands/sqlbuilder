package sqlbuilder

import "github.com/valyala/bytebufferpool"

/*
Statement provides a set of helper methods for SQL statement building and execution.
Use one of the following methods to create a SQL statement builder instance:
	sqlbuilder.From("table")
	sqlbuilder.Select("field")
	sqlbuilder.InsertInto("table")
	sqlbuilder.UpdateUser("table")
	sqlbuilder.DeleteFrom("table")
For other SQL statements use New:
	q := sqlbuilder.New("TRUNCATE")
	for _, table := range tablesToBeEmptied {
		stmt.Expr(table)
	}
	err := stmt.ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
type Statement interface {
	String() string
	Args() []interface{}
	Dest() []interface{}
	Invalidate()
	Close()
}

type statement struct {
	dialect  Dialect
	position int
	parts    []statementPart
	buffer   *bytebufferpool.ByteBuffer
	sql      *bytebufferpool.ByteBuffer
	args     []interface{}
	dest     []interface{}
}

type statementPart struct {
	position int
	bufLow   int
	bufHigh  int
	hasExpr  bool
	argLen   int
}

func getBuffer() *bytebufferpool.ByteBuffer {
	return bytebufferpool.Get()
}

func putBuffer(buf *bytebufferpool.ByteBuffer) {
	bytebufferpool.Put(buf)
}

func newStmt() *statement {
	return &statement{
		parts: make([]statementPart, 0, 8),
	}
}

func getStmt(d Dialect) *statement {
	stmt := newStmt()
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

	// stmtPool.Put(q)
}

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

	// Find the position to insert a chunk to
loop:
	for i := index - 1; i >= 0; i-- {
		chunk := &stmt.parts[i]
		index = i
		switch {
		// See if an existing chunk can be extended
		case chunk.position == pos:
			// Do nothing if a clause is already there and no expressions are to be added
			if expr == "" {
				// See if arguments are to be updated
				if argLen > 0 {
					copy(stmt.args[len(stmt.args)-argTail-chunk.argLen:], args)
				}
				return i
			}
			// Write a separator
			if chunk.hasExpr {
				stmt.buffer.WriteString(sep)
			} else {
				stmt.buffer.WriteString(" ")
			}
			if chunk.bufHigh == bufLow {
				// Do not add a chunk
				addNew = false
				// UpdateUser the existing one
				stmt.buffer.WriteString(expr)
				chunk.argLen += argLen
				chunk.bufHigh = len(stmt.buffer.B)
				chunk.hasExpr = true
			} else {
				// Do not add a clause
				addClause = false
				index = i + 1
			}
			break loop
		// No existing chunks of this type
		case chunk.position < pos:
			index = i + 1
			break loop
		default:
			argTail += chunk.argLen
		}
	}

	if addNew {
		// Insert a new chunk
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

		chunk := statementPart{
			position: pos,
			bufLow:   bufLow,
			bufHigh:  len(stmt.buffer.B),
			argLen:   argLen,
			hasExpr:  expr != "",
		}

		stmt.parts = append(stmt.parts, chunk)
		if index < len(stmt.parts)-1 {
			copy(stmt.parts[index+1:], stmt.parts[index:])
			stmt.parts[index] = chunk
		}
	}

	// Insert query arguments
	if argLen > 0 {
		stmt.args = insertAt(stmt.args, args, len(stmt.args)-argTail)
	}
	stmt.Invalidate()

	return index
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

/*
New initializes a SQL statement builder instance with an arbitrary verb.
Use sqlbuilder.Select(), sqlbuilder.InsertInto(), sqlbuilder.DeleteFrom() to start
common SQL statements.
Use New for special cases like this:
	q := sqlbuilder.New("TRANCATE")
	for _, table := range tableNames {
		stmt.Expr(table)
	}
	stmt.Clause("RESTART IDENTITY")
	err := stmt.ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
func New(verb string, args ...interface{}) Statement {
	return selectedDialect.New(verb, args...)
}

// String method builds and returns an SQL statement.
func (stmt *statement) String() string {
	if stmt.sql == nil {
		var argNo int = 1
		// Build a query
		buf := getBuffer()
		stmt.sql = buf

		pos := 0
		for n, part := range stmt.parts {
			// Separate clauses with spaces
			if n > 0 && part.position > pos {
				buf.Write(space)
			}
			s := stmt.buffer.B[part.bufLow:part.bufHigh]
			if part.argLen > 0 && stmt.dialect == PostgreSQL {
				argNo, _ = writePostgresql(argNo, s, buf)
			} else {
				buf.Write(s)
			}
			pos = part.position
		}
	}
	return bufferToString(&stmt.sql.B)
}

/*
Args returns the list of arguments to be passed to
database driver for statement execution.
Do not access a slice returned by this method after Stmt is closed.
An array, a returned slice points to, can be altered by any method that
adds a clause or an expression with arguments.
Make sure to make a copy of the returned slice if you need to preserve it.
*/
func (stmt *statement) Args() []interface{} {
	return stmt.args
}

/*
Dest returns a list of value pointers passed via To method calls.
The order matches the constructed SQL statement.
Do not access a slice returned by this method after Stmt is closed.
Note that an array, a returned slice points to, can be altered by To method
calls.
Make sure to make a copy if you need to preserve a slice returned by this method.
*/
func (stmt *statement) Dest() []interface{} {
	return stmt.dest
}

/*
Invalidate forces a rebuild on next query execution.
Most likely you don't need to call this method directly.
*/
func (stmt *statement) Invalidate() {
	if stmt.sql != nil {
		putBuffer(stmt.sql)
		stmt.sql = nil
	}
}

/*
Close puts buffers and other objects allocated to build an SQL statement
back to pool for reuse by other Stmt instances.
Stmt instance should not be used after Close method call.
*/
func (stmt *statement) Close() {
	reuseStmt(stmt)
}

/*
Select adds a SELECT clause to a statement and/or appends
an expression that defines columns of a resulting data set.
	stmt := sqlbuilder.Select("DISTINCT field1, field2").From("table")
Select can be called multiple times to add more columns:
	stmt := sqlbuilder.From("table").Select("field1")
	if needField2 {
		stmt.Select("field2")
	}
	// ...
	stmt.Close()
Use To method to bind variables to selected columns:
	var (
		num  int
		name string
	)
	res := sqlbuilder.From("table").
		Select("num, name").To(&num, &name).
		Where("id = ?", 42).
		QueryRowAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
Note that a SELECT statement can also be started by a From method call.
*/
func (stmt *statement) Select(expr string, args ...interface{}) Statement {
	stmt.addPart(posSelect, "SELECT", expr, args, ", ")
	return stmt
}

/*
UpdateUser adds UPDATE clause to a statement.
	stmt.UpdateUser("table")
tableName argument can be a SQL fragment:
	stmt.UpdateUser("ONLY table AS t")
*/
func (stmt *statement) Update(tableName string) Statement {
	stmt.addPart(posUpdate, "UPDATE", tableName, nil, ", ")
	return stmt
}

/*
InsertInto adds INSERT INTO clause to a statement.
	stmt.InsertInto("table")
tableName argument can be a SQL fragment:
	stmt.InsertInto("table AS t")
*/
func (stmt *statement) InsertInto(tableName string) Statement {
	stmt.addPart(posInsert, "INSERT INTO", tableName, nil, ", ")
	stmt.addPart(posInsertFields-1, "(", "", nil, "")
	stmt.addPart(posValues-1, ") VALUES (", "", nil, "")
	stmt.addPart(posValues+1, ")", "", nil, "")
	stmt.position = posInsertFields
	return stmt
}

/*
DeleteFrom adds DELETE clause to a statement.
	stmt.DeleteFrom("table").Where("id = ?", id)
*/
func (stmt *statement) DeleteFrom(tableName string) Statement {
	stmt.addPart(posDelete, "DELETE FROM", tableName, nil, ", ")
	return stmt
}
