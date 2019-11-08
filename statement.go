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
		q.Expr(table)
	}
	err := q.ExecAndClose(ctx, db)
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

// addChunk adds a clause or expression to a statement.
func (stmt *statement) addChunk(pos int, clause, expr string, args []interface{}, sep string) (index int) {
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
		q.Expr(table)
	}
	q.Clause("RESTART IDENTITY")
	err := q.ExecAndClose(ctx, db)
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
