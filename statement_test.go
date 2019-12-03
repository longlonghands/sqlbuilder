package sqlbuilder_test

import (
	"fmt"
	"sqlbuilder"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBuilder(t *testing.T) {
	stmt := sqlbuilder.New("SELECT *").From("table")
	defer stmt.Close()
	sql := stmt.String()
	args := stmt.Args()
	assert.Equal(t, "SELECT * FROM table", sql)
	assert.Empty(t, args)
}

func TestPgPlaceholders(t *testing.T) {
	stmt := sqlbuilder.UsingPostgresql().From("series").
		Select("id").
		Where("time > ?", time.Now().Add(time.Hour*-24*14)).
		Where("(time < ?)", time.Now().Add(time.Hour*-24*7))
	defer stmt.Close()
	sql, _ := stmt.String(), stmt.Args()
	assert.Equal(t, "SELECT id FROM series WHERE time > $1 AND (time < $2)", sql)
}

func TestPgPlaceholderEscape(t *testing.T) {
	stmt := sqlbuilder.UsingPostgresql().From("series").
		Select("id").
		Where("time \\?> ? + 1", time.Now().Add(time.Hour*-24*14)).
		Where("time < ?", time.Now().Add(time.Hour*-24*7))
	defer stmt.Close()
	sql, _ := stmt.String(), stmt.Args()
	assert.Equal(t, "SELECT id FROM series WHERE time ?> $1 + 1 AND time < $2", sql)
}

func TestBasicSelect(t *testing.T) {

	q := sqlbuilder.From("table").Select("id").Where("id > ?", 42).Where("id < ?", 1000)
	defer q.Close()
	sql, args := q.String(), q.Args()
	assert.Equal(t, "SELECT id FROM table WHERE id > ? AND id < ?", sql)
	assert.Equal(t, []interface{}{42, 1000}, args)
}

func TestMixedOrder(t *testing.T) {
	q := sqlbuilder.Select("id").Where("id > ?", 42).From("table").Where("id < ?", 1000)
	defer q.Close()
	sql, args := q.String(), q.Args()
	assert.Equal(t, "SELECT id FROM table WHERE id > ? AND id < ?", sql)
	assert.Equal(t, []interface{}{42, 1000}, args)
}

func TestClause(t *testing.T) {
	q := sqlbuilder.Select("id").From("table").Where("id > ?", 42).Clause("FETCH NEXT").Clause("FOR UPDATE")
	defer q.Close()
	sql, args := q.String(), q.Args()
	assert.Equal(t, "SELECT id FROM table WHERE id > ? FETCH NEXT FOR UPDATE", sql)
	assert.Equal(t, []interface{}{42}, args)
}

func TestExpr(t *testing.T) {
	q := sqlbuilder.From("table").
		Select("id").
		Expr("(select 1 from related where table_id = table.id limit 1) AS has_related").
		Where("id > ?", 42)
	assert.Equal(t, "SELECT id, (select 1 from related where table_id = table.id limit 1) AS has_related FROM table WHERE id > ?", q.String())
	assert.Equal(t, []interface{}{42}, q.Args())
	q.Close()
}

func TestManyFields(t *testing.T) {
	q := sqlbuilder.Select("id").From("table").Where("id = ?", 42)
	defer q.Close()
	for i := 1; i <= 3; i++ {
		q.Select(fmt.Sprintf("(id + ?) as id_%d", i), i*10)
	}
	for _, field := range []string{"uno", "dos", "tres"} {
		q.Select(field)
	}
	sql, args := q.String(), q.Args()
	assert.Equal(t, "SELECT id, (id + ?) as id_1, (id + ?) as id_2, (id + ?) as id_3, uno, dos, tres FROM table WHERE id = ?", sql)
	assert.Equal(t, []interface{}{10, 20, 30, 42}, args)
}

func TestEvenMoreFields(t *testing.T) {
	q := sqlbuilder.Select("id").From("table")
	defer q.Close()
	for n := 1; n <= 50; n++ {
		q.Select(fmt.Sprintf("field_%d", n))
	}
	sql, args := q.String(), q.Args()
	assert.Equal(t, 0, len(args))
	for n := 1; n <= 50; n++ {
		field := fmt.Sprintf(", field_%d", n)
		assert.Contains(t, sql, field)
	}
}

func TestTo(t *testing.T) {
	var (
		field1 int
		field2 string
	)
	q := sqlbuilder.From("table").
		Select("field1").To(&field1).
		Select("field2").To(&field2)
	defer q.Close()
	dest := q.Dest()

	assert.Equal(t, []interface{}{&field1, &field2}, dest)
}

func TestManyClauses(t *testing.T) {
	q := sqlbuilder.From("table").
		Select("field").
		Where("id > ?", 2).
		Clause("UNO").
		Clause("DOS").
		Clause("TRES").
		Clause("QUATRO").
		Offset(10).
		Limit(5).
		Clause("NO LOCK")
	defer q.Close()
	sql, args := q.String(), q.Args()

	assert.Equal(t, "SELECT field FROM table WHERE id > ? UNO DOS TRES QUATRO LIMIT ? OFFSET ? NO LOCK", sql)
	assert.Equal(t, []interface{}{2, 5, 10}, args)
}

func TestWithRecursive(t *testing.T) {
	q := sqlbuilder.From("orders").
		With("RECURSIVE regional_sales", sqlbuilder.From("orders").Select("region, SUM(amount) AS total_sales").GroupBy("region")).
		With("top_regions", sqlbuilder.From("regional_sales").Select("region").OrderBy("total_sales DESC").Limit(5)).
		Select("region").
		Select("product").
		Select("SUM(quantity) AS product_units").
		Select("SUM(amount) AS product_sales").
		Where("region IN (SELECT region FROM top_regions)").
		GroupBy("region, product")
	defer q.Close()

	assert.Equal(t, "WITH RECURSIVE regional_sales AS (SELECT region, SUM(amount) AS total_sales FROM orders GROUP BY region), top_regions AS (SELECT region FROM regional_sales ORDER BY total_sales DESC LIMIT ?) SELECT region, product, SUM(quantity) AS product_units, SUM(amount) AS product_sales FROM orders WHERE region IN (SELECT region FROM top_regions) GROUP BY region, product", q.String())
}

func TestSubQueryDialect(t *testing.T) {
	q := sqlbuilder.UsingPostgresql().From("users u").
		Select("email").
		Where("registered > ?", "2019-01-01").
		SubQuery("EXISTS (", ")",
			sqlbuilder.UsingPostgresql().From("orders").
				Select("id").
				Where("user_id = u.id").
				Where("amount > ?", 100))
	defer q.Close()

	// Parameter placeholder numbering should match the arguments
	assert.Equal(t, "SELECT email FROM users u WHERE registered > $1 AND EXISTS (SELECT id FROM orders WHERE user_id = u.id AND amount > $2)", q.String())
	assert.Equal(t, []interface{}{"2019-01-01", 100}, q.Args())
}

func TestClone(t *testing.T) {
	var (
		value  string
		value2 string
	)
	q := sqlbuilder.From("table").Select("field").To(&value).Where("id = ?", 42)
	defer q.Close()

	assert.Equal(t, "SELECT field FROM table WHERE id = ?", q.String())

	q2 := q.Clone()
	defer q2.Close()

	assert.Equal(t, q.Args(), q2.Args())
	assert.Equal(t, q.Dest(), q2.Dest())
	assert.Equal(t, q.String(), q2.String())

	q2.Where("time < ?", time.Now())

	assert.Equal(t, q.Dest(), q2.Dest())
	assert.NotEqual(t, q.Args(), q2.Args())
	assert.NotEqual(t, q.String(), q2.String())

	q2.Select("field2").To(&value2)
	assert.NotEqual(t, q.Dest(), q2.Dest())
	assert.NotEqual(t, q.Args(), q2.Args())
	assert.NotEqual(t, q.String(), q2.String())

	// Add more clauses to original statement to re-allocate chunks array
	q.With("top_users", sqlbuilder.From("users").OrderBy("rating DESCT").Limit(10)).
		GroupBy("id").
		Having("field > ?", 10).
		Paginate(1, 20).
		Clause("FETCH ROWS ONLY").
		Clause("FOR UPDATE")

	q3 := q.Clone()
	assert.Equal(t, q.Args(), q3.Args())
	assert.Equal(t, q.Dest(), q3.Dest())
	assert.Equal(t, q.String(), q3.String())

	assert.NotEqual(t, q.Dest(), q2.Dest())
	assert.NotEqual(t, q.Args(), q2.Args())
	assert.NotEqual(t, q.String(), q2.String())
}

func TestJoin(t *testing.T) {
	q := sqlbuilder.From("orders o").Select("id").Join("users u", "u.id = o.user_id")
	defer q.Close()
	assert.Equal(t, "SELECT id FROM orders o JOIN users u ON (u.id = o.user_id)", q.String())
}

func TestLeftJoin(t *testing.T) {
	q := sqlbuilder.From("orders o").Select("id").LeftJoin("users u", "u.id = o.user_id")
	defer q.Close()
	assert.Equal(t, "SELECT id FROM orders o LEFT JOIN users u ON (u.id = o.user_id)", q.String())
}

func TestRightJoin(t *testing.T) {
	q := sqlbuilder.From("orders o").Select("id").RightJoin("users u", "u.id = o.user_id")
	defer q.Close()
	assert.Equal(t, "SELECT id FROM orders o RIGHT JOIN users u ON (u.id = o.user_id)", q.String())
}

func TestFullJoin(t *testing.T) {
	q := sqlbuilder.From("orders o").Select("id").FullJoin("users u", "u.id = o.user_id")
	defer q.Close()
	assert.Equal(t, "SELECT id FROM orders o FULL JOIN users u ON (u.id = o.user_id)", q.String())
}

func TestUnion(t *testing.T) {
	q := sqlbuilder.From("tasks").
		Select("id, status").
		Where("status = ?", "new").
		Union(false, sqlbuilder.From("tasks").
			Select("id, status").
			Where("status = ?", "wip"))
	defer q.Close()
	assert.Equal(t, "SELECT id, status FROM tasks WHERE status = ? UNION SELECT id, status FROM tasks WHERE status = ?", q.String())
}

func TestLimit(t *testing.T) {
	q := sqlbuilder.From("items").
		Select("id").
		Where("id > ?", 42).
		Limit(10).
		Limit(11).
		Limit(20)
	defer q.Close()
	assert.Equal(t, "SELECT id FROM items WHERE id > ? LIMIT ?", q.String())
	assert.Equal(t, []interface{}{42, 20}, q.Args())
}
