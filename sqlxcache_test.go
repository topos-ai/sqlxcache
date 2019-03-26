package sqlxcache

import (
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	testDSN      = `postgres://localhost/sqlxcache?sslmode=disable`
	testDatabase = `
DROP SCHEMA IF EXISTS test CASCADE;
CREATE SCHEMA test;

CREATE TABLE test.test_object (
	id integer PRIMARY KEY
);

INSERT INTO test.test_object (id) VALUES (0);
`
)

type TestObject struct {
	ID int `db:"id"`
}

func BenchmarkSqlxQueryx(b *testing.B) {
	db, err := sqlx.Open("postgres", testDSN)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	}()

	if _, err := db.Exec(testDatabase); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testObject := &TestObject{}
		if err := db.Get(testObject, "SELECT id FROM test.test_object WHERE id = $1", 0); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSqlxPrepareQueryx(b *testing.B) {
	db, err := sqlx.Open("postgres", testDSN)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	}()

	if _, err := db.Exec(testDatabase); err != nil {
		b.Fatal(err)
	}

	stmt, err := db.Preparex("SELECT id FROM test.test_object WHERE id = $1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testObject := &TestObject{}
		if err := stmt.Get(testObject, 0); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkCacheQueryx(b *testing.B) {
	db, err := Open("postgres", testDSN)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	}()

	if _, err := db.db.Exec(testDatabase); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testObject := &TestObject{}
		if err := db.Get(testObject, "SELECT id FROM test.test_object WHERE id = $1", 0); err != nil {
			b.Error(err)
		}
	}
}
