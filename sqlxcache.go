package sqlxcache

import (
	"context"
	"database/sql"
	"sync"

	"github.com/jmoiron/sqlx"
)

type Cache struct {
	db                        *sqlx.DB
	stmtsLock, namedStmtsLock sync.Mutex
	stmts                     map[string]*sqlx.Stmt
	namedStmts                map[string]*sqlx.NamedStmt
}

func New(db *sqlx.DB) *Cache {
	return &Cache{
		db:         db,
		stmts:      map[string]*sqlx.Stmt{},
		namedStmts: map[string]*sqlx.NamedStmt{},
	}
}

func (c *Cache) stmt(query string) (*sqlx.Stmt, error) {
	c.stmtsLock.Lock()
	defer c.stmtsLock.Unlock()

	value, ok := c.stmts[query]
	if ok {
		return value, nil
	}

	stmt, err := c.db.Preparex(query)
	if err != nil {
		return nil, err
	}

	c.stmts[query] = stmt
	return stmt, nil
}

func (c *Cache) stmtContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	c.stmtsLock.Lock()
	defer c.stmtsLock.Unlock()

	value, ok := c.stmts[query]
	if ok {
		return value, nil
	}

	stmt, err := c.db.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}

	c.stmts[query] = stmt
	return stmt, nil
}

func (c *Cache) namedStmt(query string) (*sqlx.NamedStmt, error) {
	c.namedStmtsLock.Lock()
	defer c.namedStmtsLock.Unlock()

	value, ok := c.namedStmts[query]
	if ok {
		return value, nil
	}

	namedStmt, err := c.db.PrepareNamed(query)
	if err != nil {
		return nil, err
	}

	c.namedStmts[query] = namedStmt
	return namedStmt, nil
}

func (c *Cache) namedStmtContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	c.namedStmtsLock.Lock()
	defer c.namedStmtsLock.Unlock()

	value, ok := c.namedStmts[query]
	if ok {
		return value, nil
	}

	namedStmt, err := c.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, err
	}

	c.namedStmts[query] = namedStmt
	return namedStmt, nil
}

func Open(driverName, dataSourceName string) (*Cache, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return New(db), nil
}

type Tx struct {
	c  *Cache
	tx *sqlx.Tx
}

func (c *Cache) Begin() (*Tx, error) {
	tx, err := c.db.Beginx()
	if err != nil {
		return nil, err
	}

	return &Tx{
		c:  c,
		tx: tx,
	}, nil
}

func (c *Cache) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{
		c:  c,
		tx: tx,
	}, nil
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := tx.c.stmt(query)
	if err != nil {
		return nil, err
	}

	return tx.tx.Stmtx(stmt).Exec(args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := tx.c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtxContext(ctx, stmt).ExecContext(ctx, args...)
}

func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	namedStmt, err := tx.c.namedStmt(query)
	if err != nil {
		return nil, err
	}

	return tx.tx.NamedStmt(namedStmt).Exec(arg)
}

func (tx *Tx) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	namedStmt, err := tx.c.namedStmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.NamedStmtContext(ctx, namedStmt).ExecContext(ctx, arg)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := tx.c.stmt(query)
	if err != nil {
		return nil, err
	}

	return tx.tx.Stmtx(stmt).Query(args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := tx.c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtxContext(ctx, stmt).QueryContext(ctx, args...)
}

func (tx *Tx) NamedQuery(query string, arg interface{}) (*sql.Rows, error) {
	namedStmt, err := tx.c.namedStmt(query)
	if err != nil {
		return nil, err
	}

	return tx.tx.Stmtx(namedStmt).Query(arg)
}

func (tx *Tx) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sql.Rows, error) {
	namedStmt, err := tx.c.namedStmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtxContext(ctx, namedStmt).QueryContext(ctx, arg)
}

func (tx *Tx) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	stmt, err := tx.c.stmt(query)
	if err != nil {
		return nil, err
	}

	return tx.tx.Stmtx(stmt).Queryx(args...)
}

func (tx *Tx) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	stmt, err := tx.c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtxContext(ctx, stmt).QueryxContext(ctx, args...)
}

func (tx *Tx) NamedQueryx(query string, arg interface{}) (*sqlx.Rows, error) {
	namedStmt, err := tx.c.namedStmt(query)
	if err != nil {
		return nil, err
	}

	return tx.tx.Stmtx(namedStmt).Queryx(arg)
}

func (tx *Tx) NamedQueryxContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	namedStmt, err := tx.c.namedStmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtxContext(ctx, namedStmt).QueryxContext(ctx, arg)
}

func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) error {
	stmt, err := tx.c.stmt(query)
	if err != nil {
		return err
	}

	return tx.tx.Stmtx(stmt).Get(dest, args...)
}

func (tx *Tx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	stmt, err := tx.c.stmtContext(ctx, query)
	if err != nil {
		return err
	}

	return tx.tx.StmtxContext(ctx, stmt).GetContext(ctx, dest, args...)
}

func (tx *Tx) GetNamed(dest interface{}, query string, arg interface{}) error {
	namedStmt, err := tx.c.namedStmt(query)
	if err != nil {
		return err
	}

	return tx.tx.NamedStmt(namedStmt).Get(dest, arg)
}

func (tx *Tx) GetNamedContext(ctx context.Context, dest interface{}, query string, arg interface{}) error {
	namedStmt, err := tx.c.namedStmtContext(ctx, query)
	if err != nil {
		return err
	}

	return tx.tx.NamedStmtContext(ctx, namedStmt).GetContext(ctx, dest, arg)
}

func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	stmt, err := tx.c.stmt(query)
	if err != nil {
		return err
	}

	return tx.tx.Stmtx(stmt).Select(dest, args...)
}

func (tx *Tx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	stmt, err := tx.c.stmtContext(ctx, query)
	if err != nil {
		return err
	}

	return tx.tx.StmtxContext(ctx, stmt).SelectContext(ctx, dest, args...)
}

func (tx *Tx) NamedSelect(dest interface{}, query string, arg interface{}) error {
	namedStmt, err := tx.c.namedStmt(query)
	if err != nil {
		return err
	}

	return tx.tx.NamedStmt(namedStmt).Select(dest, arg)
}

func (tx *Tx) NamedSelectContext(ctx context.Context, dest interface{}, query string, arg interface{}) error {
	namedStmt, err := tx.c.namedStmtContext(ctx, query)
	if err != nil {
		return err
	}

	return tx.tx.NamedStmtContext(ctx, namedStmt).SelectContext(ctx, dest, arg)
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (c *Cache) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := c.stmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.Exec(args...)
}

func (c *Cache) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.ExecContext(ctx, args...)
}

func (c *Cache) NamedExec(query string, arg interface{}) (sql.Result, error) {
	namedStmt, err := c.namedStmt(query)
	if err != nil {
		return nil, err
	}

	return namedStmt.Exec(arg)
}

func (c *Cache) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	namedStmt, err := c.namedStmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return namedStmt.ExecContext(ctx, arg)
}

func (c *Cache) QueryRow(query string, args ...interface{}) (*sql.Row, error) {
	stmt, err := c.stmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRow(args...), nil
}

func (c *Cache) QueryRowContext(ctx context.Context, query string, args ...interface{}) (*sql.Row, error) {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRowContext(ctx, args...), nil
}

func (c *Cache) QueryxRow(query string, args ...interface{}) (*sqlx.Row, error) {
	stmt, err := c.stmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRowx(args...), nil
}

func (c *Cache) QueryRowxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Row, error) {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryRowxContext(ctx, args...), nil
}

func (c *Cache) NamedQueryRow(query string, arg interface{}) (*sqlx.Row, error) {
	namedStmt, err := c.namedStmt(query)
	if err != nil {
		return nil, err
	}

	return namedStmt.QueryRow(arg), nil
}

func (c *Cache) NamedQueryRowContext(ctx context.Context, query string, arg interface{}) (*sqlx.Row, error) {
	namedStmt, err := c.namedStmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return namedStmt.QueryRowContext(ctx, arg), nil
}

func (c *Cache) Query(query string, arg interface{}) (*sql.Rows, error) {
	stmt, err := c.stmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.Query(arg)
}

func (c *Cache) QueryContext(ctx context.Context, query string, arg interface{}) (*sql.Rows, error) {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryContext(ctx, arg)
}

func (c *Cache) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	namedStmt, err := c.namedStmt(query)
	if err != nil {
		return nil, err
	}

	return namedStmt.Queryx(arg)
}

func (c *Cache) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	namedStmt, err := c.namedStmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return namedStmt.QueryxContext(ctx, arg)
}

func (c *Cache) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	stmt, err := c.stmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.Queryx(args...)
}

func (c *Cache) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryxContext(ctx, args...)
}

func (c *Cache) Get(dest interface{}, query string, args ...interface{}) error {
	stmt, err := c.stmt(query)
	if err != nil {
		return err
	}

	return stmt.Get(dest, args...)
}

func (c *Cache) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return err
	}

	return stmt.GetContext(ctx, dest, args...)
}

func (c *Cache) NamedGet(dest interface{}, query string, arg interface{}) error {
	namedStmt, err := c.namedStmt(query)
	if err != nil {
		return err
	}

	return namedStmt.Get(dest, arg)
}

func (c *Cache) NamedGetContext(ctx context.Context, dest interface{}, query string, arg interface{}) error {
	namedStmt, err := c.namedStmtContext(ctx, query)
	if err != nil {
		return err
	}

	return namedStmt.GetContext(ctx, dest, arg)
}

func (c *Cache) Select(dest interface{}, query string, args ...interface{}) error {
	stmt, err := c.stmt(query)
	if err != nil {
		return err
	}

	return stmt.Select(dest, args...)
}

func (c *Cache) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	stmt, err := c.stmtContext(ctx, query)
	if err != nil {
		return err
	}

	return stmt.SelectContext(ctx, dest, args...)
}

func (c *Cache) NamedSelect(dest interface{}, query string, arg interface{}) error {
	namedStmt, err := c.namedStmt(query)
	if err != nil {
		return err
	}

	return namedStmt.Select(dest, arg)
}

func (c *Cache) NamedSelectContext(ctx context.Context, dest interface{}, query string, arg interface{}) error {
	namedStmt, err := c.namedStmtContext(ctx, query)
	if err != nil {
		return err
	}

	return namedStmt.SelectContext(ctx, dest, arg)
}

func (c *Cache) DB() *sqlx.DB {
	return c.db
}

func (c *Cache) Close() error {
	return c.db.Close()
}
