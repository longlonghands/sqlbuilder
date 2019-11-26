package sqlbuilder

import (
	"strings"

	"github.com/valyala/bytebufferpool"
)

/*
Statement provides a set of helper methods for SQL statement building and execution.
Use one of the following methods to create a SQL statement builder instance:
	sqlbuilder.From("table")
	sqlbuilder.Select("field")
	sqlbuilder.InsertInto("table")
	sqlbuilder.UpdateUser("table")
	sqlbuilder.DeleteFrom("table")
For other SQL statements use New:
	stmt := sqlbuilder.New("TRUNCATE")
	for _, table := range tablesToBeEmptied {
		stmt.Expr(table)
	}
	err := stmt.ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
type Statement interface {
	// String method builds and returns an SQL statement.
	String() string

	// GetDialect returns selected dialect in the library
	GetDialect() Dialect

	// SetDialect sets value as selected dialect in the library
	SetDialect(value Dialect)

	/*
		Args returns the list of arguments to be passed to
		database driver for statement execution.
		Do not access a slice returned by this method after Stmt is closed.
		An array, a returned slice points to, can be altered by any method that
		adds a clause or an expression with arguments.
		Make sure to make a copy of the returned slice if you need to preserve it.
	*/
	Args() []interface{}

	/*
		Dest returns a list of value pointers passed via To method calls.
		The order matches the constructed SQL statement.
		Do not access a slice returned by this method after Stmt is closed.
		Note that an array, a returned slice points to, can be altered by To method
		calls.
		Make sure to make a copy if you need to preserve a slice returned by this method.
	*/
	Dest() []interface{}

	/*
		Invalidate forces a rebuild on next query execution.
		Most likely you don't need to call this method directly.
	*/
	Invalidate()

	/*
		Close puts buffers and other objects allocated to build an SQL statement
		back to pool for reuse by other Stmt instances.
		Stmt instance should not be used after Close method call.
	*/
	Close()

	// Clone creates a copy of the statement.
	Clone() Statement

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
	Select(expr string, args ...interface{}) Statement

	/*
		UpdateUser adds UPDATE clause to a statement.
			stmt.UpdateUser("table")
		tableName argument can be a SQL fragment:
			stmt.UpdateUser("ONLY table AS t")
	*/
	Update(tableName string) Statement

	/*
		InsertInto adds INSERT INTO clause to a statement.
			stmt.InsertInto("table")
		tableName argument can be a SQL fragment:
			stmt.InsertInto("table AS t")
	*/
	InsertInto(tableName string) Statement

	/*
		DeleteFrom adds DELETE clause to a statement.
			stmt.DeleteFrom("table").Where("id = ?", id)
	*/
	DeleteFrom(tableName string) Statement

	/*
		Set method:
		- Adds a column to the list of columns and a value to VALUES clause of INSERT statement,
		- Adds an item to SET clause of an UPDATE statement.
			stmt.Set("field", 32)
		For INSERT statements a call to Set method generates
		both the list of columns and values to be inserted:
			stmt := sqlbuilder.InsertInto("table").Set("field", 42)
		produces
			INSERT INTO table (field) VALUES (42)
	*/
	Set(field string, value interface{}) Statement

	/*
		SetExpr is an extended version of a Set method.
			stmt.SetExpr("field", "field + 1")
			stmt.SetExpr("field", "? + ?", 31, 11)
	*/
	SetExpr(field, expr string, args ...interface{}) Statement

	/*
		From starts a SELECT statement.
			var cnt int64
			err := sqlbuilder.From("table").
				Select("COUNT(*)").To(&cnt)
				Where("value >= ?", 42).
				QueryRowAndClose(ctx, db)
			if err != nil {
				panic(err)
			}
	*/
	From(expr string, args ...interface{}) Statement

	/*
		Where adds a filter:
			sqlbuilder.From("users").
				Select("id, name").
				Where("email = ?", email).
				Where("is_active = 1")
	*/
	Where(expr string, args ...interface{}) Statement

	/*
		In adds IN expression to the current filter.
		In method must be called after a Where method call.
	*/
	In(args ...interface{}) Statement

	// OrderBy adds the ORDER BY clause to SELECT statement
	OrderBy(expr ...string) Statement

	// GroupBy adds the GROUP BY clause to SELECT statement
	GroupBy(expr string) Statement

	// Having adds the HAVING clause to SELECT statement
	Having(expr string, args ...interface{}) Statement

	// Limit adds a limit on number of returned rows
	Limit(limit interface{}) Statement

	// Offset adds a limit on number of returned rows
	Offset(offset interface{}) Statement

	// Paginate provides an easy way to set both offset and limit
	Paginate(page, pageSize int) Statement

	// Join adds an INNERT JOIN clause to SELECT statement
	Join(table, on string) Statement

	// LeftJoin adds a LEFT OUTER JOIN clause to SELECT statement
	LeftJoin(table, on string) Statement

	// RightJoin adds a RIGHT OUTER JOIN clause to SELECT statement
	RightJoin(table, on string) Statement

	// FullJoin adds a FULL OUTER JOIN clause to SELECT statement
	FullJoin(table, on string) Statement

	// Returning adds a RETURNING clause to a statement
	Returning(expr string) Statement

	/*
		With prepends a statement with an WITH clause.
		With method calls a Close method of a given query, so
		make sure not to reuse it afterwards.
	*/
	With(queryName string, query Statement) Statement

	/*
		Expr appends an expression to the most recently added clause.
		Expressions are separated with commas.
	*/
	Expr(expr string, args ...interface{}) Statement

	/*
		SubQuery appends a sub query expression to a current clause.
		SubQuery method call closes the Stmt passed as query parameter.
		Do not reuse it afterwards.
	*/
	SubQuery(prefix, suffix string, query Statement) Statement

	/*
		Union adds a UNION clause to the statement.
		all argument controls if UNION ALL or UNION clause
		is to be constructed. Use UNION ALL if possible to
		get faster queries.
	*/
	Union(all bool, query Statement) Statement

	/*
		Clause appends a raw SQL fragment to the statement.

		Use it to add a raw SQL fragment like ON CONFLICT, ON DUPLICATE KEY, WINDOW, etc.

		An SQL fragment added via Clause method appears after the last clause previously
		added. If called first, Clause method prepends a statement with a raw SQL.
	*/
	Clause(expr string, args ...interface{}) Statement
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

/*
WithDialect initializes a SQL statement builder instance with a given Dialect.
Use WithDialect like this:
	var cnt int64
	err := sqlbuilder.WithDialect(sqlbuilder.PostgreSQL).
		Select("COUNT(*)").To(&cnt).
		From("table").
		Where("value >= ?", 42).
		QueryRowAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
func WithDialect(d Dialect) Statement {
	stmt := getStmt(d)
	return stmt
}

/*
New initializes a SQL statement builder instance with an arbitrary verb.
Use sqlbuilder.Select(), sqlbuilder.InsertInto(), sqlbuilder.DeleteFrom() to start
common SQL statements.
Use New for special cases like this:
	stmt := sqlbuilder.New("TRANCATE")
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
	stmt := getStmt(selectedDialect)
	stmt.addPart(posSelect, verb, "", args, ", ")
	return stmt
}

/*
From starts a SELECT statement.
	var cnt int64
	err := sqlbuilder.From("table").
		Select("COUNT(*)").To(&cnt)
		Where("value >= ?", 42).
		QueryRowAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
func From(expr string, args ...interface{}) Statement {
	stmt := getStmt(selectedDialect)
	return stmt.From(expr, args...)
}

/*
Select starts a SELECT statement.
	var cnt int64
	err := sqlbuilder.Select("COUNT(*)").To(&cnt).
		From("table").
		Where("value >= ?", 42).
		QueryRowAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
Note that From method can also be used to start a SELECT statement.
*/
func Select(expr string, args ...interface{}) Statement {
	stmt := getStmt(selectedDialect)
	return stmt.Select(expr, args...)
}

/*
Update starts an UPDATE statement.
	err := sqlbuilder.Update("table").
		Set("field1", "newvalue").
		Where("id = ?", 42).
		ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
func Update(tableName string) Statement {
	stmt := getStmt(selectedDialect)
	return stmt.Update(tableName)
}

/*
InsertInto starts an INSERT statement.
	var newId int64
	err := sqlbuilder.InsertInto("table").
		Set("field", value).
		Returning("id").To(&newId).
		ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
func InsertInto(tableName string) Statement {
	stmt := getStmt(selectedDialect)
	return stmt.InsertInto(tableName)
}

/*
DeleteFrom starts a DELETE statement.
	err := sqlbuilder.DeleteFrom("table").Where("id = ?", id).ExecAndClose(ctx, db)
*/
func DeleteFrom(tableName string) Statement {
	stmt := getStmt(selectedDialect)
	return stmt.DeleteFrom(tableName)
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

func (stmt *statement) SetDialect(value Dialect) {
	stmt.dialect = value
}

func (stmt *statement) GetDialect() Dialect {
	return stmt.dialect
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

// Clone creates a copy of the statement.
func (stmt *statement) Clone() Statement {
	newstmt := getStmt(stmt.dialect)
	if cap(newstmt.parts) < len(stmt.parts) {
		newstmt.parts = make([]statementPart, len(stmt.parts), len(stmt.parts)+2)
		copy(stmt.parts, stmt.parts)
	} else {
		newstmt.parts = append(stmt.parts, stmt.parts...)
	}
	newstmt.args = insertAt(newstmt.args, stmt.args, 0)
	newstmt.dest = insertAt(newstmt.dest, stmt.dest, 0)
	newstmt.buffer.Write(stmt.buffer.B)
	if stmt.sql != nil {
		newstmt.sql = getBuffer()
		newstmt.sql.Write(stmt.sql.B)
	}

	return newstmt
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

/*
Set method:
- Adds a column to the list of columns and a value to VALUES clause of INSERT statement,
- Adds an item to SET clause of an UPDATE statement.
	stmt.Set("field", 32)
For INSERT statements a call to Set method generates
both the list of columns and values to be inserted:
	stmt := sqlbuilder.InsertInto("table").Set("field", 42)
produces
	INSERT INTO table (field) VALUES (42)
*/
func (stmt *statement) Set(field string, value interface{}) Statement {
	return stmt.SetExpr(field, "?", value)
}

/*
SetExpr is an extended version of a Set method.
	stmt.SetExpr("field", "field + 1")
	stmt.SetExpr("field", "? + ?", 31, 11)
*/
func (stmt *statement) SetExpr(field, expr string, args ...interface{}) Statement {
	// TODO How to handle both INSERT ... VALUES and SET in ON DUPLICATE KEY UPDATE?
	p := 0
	for _, chunk := range stmt.parts {
		if chunk.position == posInsert || chunk.position == posUpdate {
			p = chunk.position
			break
		}
	}

	switch p {
	case posInsert:
		stmt.addPart(posInsertFields, "", field, nil, ", ")
		stmt.addPart(posValues, "", expr, args, ", ")
	case posUpdate:
		stmt.addPart(posSet, "SET", field+"="+expr, args, ", ")
	}
	return stmt
}

// From adds a FROM clause to statement.
func (stmt *statement) From(expr string, args ...interface{}) Statement {
	stmt.addPart(posFrom, "FROM", expr, args, ", ")
	return stmt
}

/*
Where adds a filter:
	sqlbuilder.From("users").
		Select("id, name").
		Where("email = ?", email).
		Where("is_active = 1")
*/
func (stmt *statement) Where(expr string, args ...interface{}) Statement {
	stmt.addPart(posWhere, "WHERE", expr, args, " AND ")
	return stmt
}

/*
In adds IN expression to the current filter.
In method must be called after a Where method call.
*/
func (stmt *statement) In(args ...interface{}) Statement {
	buf := bytebufferpool.Get()
	buf.WriteString("IN (")
	l := len(args) - 1
	for i := range args {
		if i < l {
			buf.Write(placeholderComma)
		} else {
			buf.Write(placeholder)
		}
	}
	buf.WriteString(")")

	stmt.addPart(posWhere, "", bufferToString(&buf.B), args, " ")

	bytebufferpool.Put(buf)
	return stmt
}

// OrderBy adds the ORDER BY clause to SELECT statement
func (stmt *statement) OrderBy(expr ...string) Statement {
	stmt.addPart(posOrderBy, "ORDER BY", strings.Join(expr, ", "), nil, ", ")
	return stmt
}

// GroupBy adds the GROUP BY clause to SELECT statement
func (stmt *statement) GroupBy(expr string) Statement {
	stmt.addPart(posGroupBy, "GROUP BY", expr, nil, ", ")
	return stmt
}

// Having adds the HAVING clause to SELECT statement
func (stmt *statement) Having(expr string, args ...interface{}) Statement {
	stmt.addPart(posHaving, "HAVING", expr, args, " AND ")
	return stmt
}

// Limit adds a limit on number of returned rows
func (stmt *statement) Limit(limit interface{}) Statement {
	stmt.addPart(posLimit, "LIMIT ?", "", []interface{}{limit}, "")
	return stmt
}

// Offset adds a limit on number of returned rows
func (stmt *statement) Offset(offset interface{}) Statement {
	stmt.addPart(posOffset, "OFFSET ?", "", []interface{}{offset}, "")
	return stmt
}

// Paginate provides an easy way to set both offset and limit
func (stmt *statement) Paginate(page, pageSize int) Statement {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 1
	}
	if page > 1 {
		stmt.Offset((page - 1) * pageSize)
	}
	stmt.Limit(pageSize)
	return stmt
}

/*
Join adds an INNERT JOIN clause to SELECT statement
*/
func (stmt *statement) Join(table, on string) Statement {
	stmt.join("JOIN ", table, on)
	return stmt
}

/*
LeftJoin adds a LEFT OUTER JOIN clause to SELECT statement
*/
func (stmt *statement) LeftJoin(table, on string) Statement {
	stmt.join("LEFT JOIN ", table, on)
	return stmt
}

/*
RightJoin adds a RIGHT OUTER JOIN clause to SELECT statement
*/
func (stmt *statement) RightJoin(table, on string) Statement {
	stmt.join("RIGHT JOIN ", table, on)
	return stmt
}

/*
FullJoin adds a FULL OUTER JOIN clause to SELECT statement
*/
func (stmt *statement) FullJoin(table, on string) Statement {
	stmt.join("FULL JOIN ", table, on)
	return stmt
}

// Returning adds a RETURNING clause to a statement
func (stmt *statement) Returning(expr string) Statement {
	stmt.addPart(posReturning, "RETURNING", expr, nil, ", ")
	return stmt
}

// With prepends a statement with an WITH clause.
// With method calls a Close method of a given query, so
// make sure not to reuse it afterwards.
func (stmt *statement) With(queryName string, query Statement) Statement {
	stmt.addPart(posWith, "WITH", "", nil, "")
	return stmt.SubQuery(queryName+" AS (", ")", query)
}

/*
Expr appends an expression to the most recently added clause.
Expressions are separated with commas.
*/
func (stmt *statement) Expr(expr string, args ...interface{}) Statement {
	stmt.addPart(stmt.position, "", expr, args, ", ")
	return stmt
}

/*
SubQuery appends a sub query expression to a current clause.
SubQuery method call closes the Stmt passed as query parameter.
Do not reuse it afterwards.
*/
func (stmt *statement) SubQuery(prefix, suffix string, query Statement) Statement {
	delimiter := ", "
	if stmt.position == posWhere {
		delimiter = " AND "
	}
	index := stmt.addPart(stmt.position, "", prefix, query.Args(), delimiter)
	chunk := &stmt.parts[index]
	// Make sure subquery is not dialect-specific.
	if query.GetDialect() != DefaultDialect {
		query.SetDialect(DefaultDialect)
		query.Invalidate()
	}
	stmt.buffer.WriteString(query.String())
	stmt.buffer.WriteString(suffix)
	chunk.bufHigh = stmt.buffer.Len()
	// Close the subquery
	query.Close()

	return stmt
}

/*
Union adds a UNION clause to the statement.
all argument controls if UNION ALL or UNION clause
is to be constructed. Use UNION ALL if possible to
get faster queries.
*/
func (stmt *statement) Union(all bool, query Statement) Statement {
	p := posUnion
	if len(stmt.parts) > 0 {
		last := (&stmt.parts[len(stmt.parts)-1]).position
		if last >= p {
			p = last + 1
		}
	}
	var index int
	if all {
		index = stmt.addPart(p, "UNION ALL ", "", query.Args(), "")
	} else {
		index = stmt.addPart(p, "UNION ", "", query.Args(), "")
	}
	chunk := &stmt.parts[index]
	// Make sure subquery is not dialect-specific.
	if query.GetDialect() != DefaultDialect {
		query.SetDialect(DefaultDialect)
		query.Invalidate()
	}
	stmt.buffer.WriteString(query.String())
	chunk.bufHigh = stmt.buffer.Len()
	// Close the subquery
	query.Close()

	return stmt
}

/*
Clause appends a raw SQL fragment to the statement.

Use it to add a raw SQL fragment like ON CONFLICT, ON DUPLICATE KEY, WINDOW, etc.

An SQL fragment added via Clause method appears after the last clause previously
added. If called first, Clause method prepends a statement with a raw SQL.
*/
func (stmt *statement) Clause(expr string, args ...interface{}) Statement {
	p := posStart
	if len(stmt.parts) > 0 {
		p = (&stmt.parts[len(stmt.parts)-1]).position + 10
	}
	stmt.addPart(p, expr, "", args, ", ")
	return stmt
}
