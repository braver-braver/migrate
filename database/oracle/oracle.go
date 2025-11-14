package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
	gora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
)

const (
	defaultPort                  = 1521
	defaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
	maxDBMSLockID                = 1073741823
)

var (
	multiStmtDelimiter = []byte(";")
)

// DefaultMigrationsTable is the name of the migrations table in the database.
var DefaultMigrationsTable = "schema_migrations"

var _ database.Driver = (*Oracle)(nil)

var (
	ErrNilConfig     = fmt.Errorf("no config")
	ErrNoServiceName = fmt.Errorf("no service name")
	ErrNoSchema      = fmt.Errorf("no schema")
)

// Config for the database driver.
type Config struct {
	MigrationsTable       string
	SchemaName            string
	ServiceName           string
	MultiStatementEnabled bool
	MultiStatementMaxSize int

	migrationsTableName   string
	migrationsTableQuoted string
}

// Oracle connection.
type Oracle struct {
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	config *Config
	lockID int64
}

func init() {
	database.Register("oracle", &Oracle{})
}

// Open creates a new database driver from a connection string.
func (o *Oracle) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	filteredURL := migrate.FilterCustomQuery(purl)

	service := strings.TrimPrefix(purl.Path, "/")
	if service == "" {
		return nil, ErrNoServiceName
	}

	host := purl.Hostname()
	if host == "" {
		host = "localhost"
	}

	port := defaultPort
	if p := purl.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return nil, err
		}
	}

	user := ""
	pass := ""
	if purl.User != nil {
		user = purl.User.Username()
		if pwd, ok := purl.User.Password(); ok {
			pass = pwd
		}
	}

	query := purl.Query()

	migrationsTable := query.Get("x-migrations-table")

	schemaName := query.Get("x-schema")

	multiStatement := false
	if v := query.Get("x-multi-statement"); v != "" {
		if multiStatement, err = strconv.ParseBool(v); err != nil {
			return nil, err
		}
	}

	multiStatementMaxSize := 0
	if v := query.Get("x-multi-statement-max-size"); v != "" {
		if multiStatementMaxSize, err = strconv.Atoi(v); err != nil {
			return nil, err
		}
	}

	options := map[string]string{
		"CONNECTION TIMEOUT": "60",
		"PREFETCH_ROWS":      "25",
	}
	for key, vals := range filteredURL.Query() {
		if len(vals) > 0 {
			options[key] = vals[len(vals)-1]
		} else {
			options[key] = ""
		}
	}

	dsn := gora.BuildUrl(host, port, service, user, pass, options)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		MigrationsTable:       migrationsTable,
		SchemaName:            schemaName,
		ServiceName:           service,
		MultiStatementEnabled: multiStatement,
		MultiStatementMaxSize: multiStatementMaxSize,
	}

	drv, err := WithInstance(db, cfg)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
		return nil, err
	}

	return drv, nil
}

// WithInstance returns a database instance from an existing database connection.
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	ctx := context.Background()

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}
	config.migrationsTableQuoted = quoteIdentifier(config.MigrationsTable)
	config.migrationsTableName = normalizeIdentifier(config.MigrationsTable)

	if config.MultiStatementMaxSize <= 0 {
		config.MultiStatementMaxSize = defaultMultiStatementMaxSize
	}

	if config.SchemaName == "" {
		const query = `SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual`
		var schema sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&schema); err != nil {
			_ = conn.Close()
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if !schema.Valid || strings.TrimSpace(schema.String) == "" {
			_ = conn.Close()
			return nil, ErrNoSchema
		}
		config.SchemaName = strings.ToUpper(strings.TrimSpace(schema.String))
	} else {
		config.SchemaName = strings.ToUpper(strings.TrimSpace(config.SchemaName))
	}

	if config.ServiceName == "" {
		const query = `SELECT SYS_CONTEXT('USERENV','DB_NAME') FROM dual`
		var name sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&name); err != nil {
			_ = conn.Close()
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if name.Valid {
			config.ServiceName = strings.TrimSpace(name.String)
		}
	}

	ora := &Oracle{
		conn:   conn,
		db:     instance,
		config: config,
	}

	lockID, err := ora.ensureLockID()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	ora.lockID = lockID

	if err := ora.ensureVersionTable(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return ora, nil
}

// Close closes the underlying database resources.
func (o *Oracle) Close() error {
	var result error
	if o.conn != nil {
		if err := o.conn.Close(); err != nil {
			result = multierror.Append(result, err)
		}
	}
	if o.db != nil {
		if err := o.db.Close(); err != nil {
			result = multierror.Append(result, err)
		}
	}
	return result
}

// Lock acquires a database level lock using DBMS_LOCK.
func (o *Oracle) Lock() error {
	return database.CasRestoreOnErr(&o.isLocked, false, true, database.ErrLocked, func() error {
		lockID, err := o.ensureLockID()
		if err != nil {
			return err
		}

		const query = `SELECT DBMS_LOCK.REQUEST(:1, 6, 0, FALSE) FROM dual`
		var status int64
		if err := o.conn.QueryRowContext(context.Background(), query, lockID).Scan(&status); err != nil {
			return wrapError(err, query)
		}

		switch status {
		case 0:
			return nil
		case 1:
			return database.ErrLocked
		default:
			return fmt.Errorf("dbms_lock.request returned status %d", status)
		}
	})
}

// Unlock releases the database lock.
func (o *Oracle) Unlock() error {
	return database.CasRestoreOnErr(&o.isLocked, true, false, database.ErrNotLocked, func() error {
		lockID, err := o.ensureLockID()
		if err != nil {
			return err
		}

		const query = `SELECT DBMS_LOCK.RELEASE(:1) FROM dual`
		var status int64
		if err := o.conn.QueryRowContext(context.Background(), query, lockID).Scan(&status); err != nil {
			return wrapError(err, query)
		}

		switch status {
		case 0:
			return nil
		case 4:
			return database.ErrNotLocked
		default:
			return fmt.Errorf("dbms_lock.release returned status %d", status)
		}
	})
}

// Run executes a migration.
func (o *Oracle) Run(migration io.Reader) error {
	if o.config.MultiStatementEnabled {
		var runErr error
		if err := multistmt.Parse(migration, multiStmtDelimiter, o.config.MultiStatementMaxSize, func(stmt []byte) bool {
			runErr = o.runStatement(stmt)
			return runErr == nil
		}); err != nil {
			return err
		}
		return runErr
	}

	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	return o.runStatement(migr)
}

func (o *Oracle) runStatement(statement []byte) error {
	query := strings.TrimSpace(string(statement))
	if query == "" {
		return nil
	}
	query = strings.TrimSuffix(query, ";")
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	ctx := context.Background()
	if _, err := o.conn.ExecContext(ctx, query); err != nil {
		return wrapExecError(err, query)
	}
	return nil
}

// SetVersion updates the schema version in the migrations table.
func (o *Oracle) SetVersion(version int, dirty bool) error {
	tx, err := o.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `DELETE FROM ` + o.config.migrationsTableQuoted
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 || (version == database.NilVersion && dirty) {
		dirtyBit := 0
		if dirty {
			dirtyBit = 1
		}

		query = `INSERT INTO ` + o.config.migrationsTableQuoted + ` (version, dirty) VALUES (:1, :2)`
		if _, err := tx.Exec(query, version, dirtyBit); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

// Version returns the currently active schema version.
func (o *Oracle) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + o.config.migrationsTableQuoted + ` WHERE ROWNUM = 1`
	var (
		versionValue sql.NullInt64
		dirtyValue   sql.NullInt64
	)
	err = o.conn.QueryRowContext(context.Background(), query).Scan(&versionValue, &dirtyValue)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		if isOracleErrorCode(err, 942) {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	default:
		if !versionValue.Valid {
			version = database.NilVersion
		} else {
			version = int(versionValue.Int64)
		}
		if dirtyValue.Valid {
			dirty = dirtyValue.Int64 == 1
		}
		return version, dirty, nil
	}
}

// Drop removes all user tables and sequences.
func (o *Oracle) Drop() (err error) {
	ctx := context.Background()

	const tablesQuery = `SELECT table_name FROM user_tables`
	tables, err := o.conn.QueryContext(ctx, tablesQuery)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tablesQuery)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	var tableNames []string
	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}
		if table != "" {
			tableNames = append(tableNames, table)
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tablesQuery)}
	}

	for _, table := range tableNames {
		drop := fmt.Sprintf(`DROP TABLE "%s" CASCADE CONSTRAINTS PURGE`, table)
		if _, err := o.conn.ExecContext(ctx, drop); err != nil && !isOracleErrorCode(err, 942) {
			return &database.Error{OrigErr: err, Query: []byte(drop)}
		}
	}

	const seqQuery = `SELECT sequence_name FROM user_sequences`
	sequences, err := o.conn.QueryContext(ctx, seqQuery)
	if err != nil {
		if isOracleErrorCode(err, 942) {
			return nil
		}
		return &database.Error{OrigErr: err, Query: []byte(seqQuery)}
	}
	defer func() {
		if errClose := sequences.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	var sequenceNames []string
	for sequences.Next() {
		var seq string
		if err := sequences.Scan(&seq); err != nil {
			return err
		}
		if seq != "" {
			sequenceNames = append(sequenceNames, seq)
		}
	}
	if err := sequences.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(seqQuery)}
	}

	for _, seq := range sequenceNames {
		drop := fmt.Sprintf(`DROP SEQUENCE "%s"`, seq)
		if _, err := o.conn.ExecContext(ctx, drop); err != nil && !isOracleErrorCode(err, 2289) {
			return &database.Error{OrigErr: err, Query: []byte(drop)}
		}
	}

	return nil
}

func (o *Oracle) ensureVersionTable() (err error) {
	if err = o.Lock(); err != nil {
		return err
	}

	defer func() {
		unlockErr := o.Unlock()
		if unlockErr != nil {
			if err == nil {
				err = unlockErr
			} else {
				err = multierror.Append(err, unlockErr)
			}
		}
	}()

	const countQuery = `SELECT COUNT(1) FROM user_tables WHERE UPPER(table_name) = :1`
	var count int
	if err := o.conn.QueryRowContext(context.Background(), countQuery, strings.ToUpper(o.config.migrationsTableName)).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(countQuery)}
	}

	if count > 0 {
		return nil
	}

	createTable := fmt.Sprintf(`CREATE TABLE %s (version NUMBER(20) NOT NULL, dirty NUMBER(1) NOT NULL)`, o.config.migrationsTableQuoted)
	if _, err := o.conn.ExecContext(context.Background(), createTable); err != nil {
		if isOracleErrorCode(err, 955) {
			return nil
		}
		return &database.Error{OrigErr: err, Query: []byte(createTable)}
	}

	return nil
}

func (o *Oracle) ensureLockID() (int64, error) {
	if o.lockID != 0 {
		return o.lockID, nil
	}

	base := strings.ToUpper(o.config.ServiceName)
	if base == "" {
		base = strings.ToUpper(o.config.migrationsTableName)
	}

	lockNameParts := []string{base}
	if o.config.SchemaName != "" {
		lockNameParts = append(lockNameParts, strings.ToUpper(o.config.SchemaName))
	}
	if o.config.migrationsTableName != "" {
		lockNameParts = append(lockNameParts, strings.ToUpper(o.config.migrationsTableName))
	}

	lockIDStr, err := database.GenerateAdvisoryLockId(lockNameParts[0], lockNameParts[1:]...)
	if err != nil {
		return 0, err
	}

	id, err := strconv.ParseInt(lockIDStr, 10, 64)
	if err != nil {
		return 0, err
	}
	if id < 0 {
		id = -id
	}
	id = id % maxDBMSLockID
	if id == 0 {
		id = 1
	}
	o.lockID = id
	return id, nil
}

func wrapExecError(err error, query string) error {
	dbErr := wrapError(err, query)
	if dbErr != nil {
		return dbErr
	}
	return err
}

func wrapError(err error, query string) error {
	if err == nil {
		return nil
	}
	var oraErr *network.OracleError
	if errors.As(err, &oraErr) {
		dbErr := database.Error{OrigErr: err, Err: fmt.Sprintf("migration failed: %s", oraErr.Error()), Query: []byte(query)}
		if pos := oraErr.ErrPos(); pos >= 0 {
			dbErr.Line = uint(pos)
		}
		return dbErr
	}
	if query == "" {
		return err
	}
	return &database.Error{OrigErr: err, Query: []byte(query)}
}

func isOracleErrorCode(err error, code int) bool {
	if err == nil {
		return false
	}
	var oraErr *network.OracleError
	if errors.As(err, &oraErr) {
		return oraErr.ErrCode == code
	}
	needle := fmt.Sprintf("ORA-%05d", code)
	return strings.Contains(err.Error(), needle)
}

func quoteIdentifier(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	if strings.HasPrefix(name, "\"") && strings.HasSuffix(name, "\"") {
		return name
	}
	return "\"" + strings.ToUpper(name) + "\""
}

func normalizeIdentifier(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	if strings.HasPrefix(name, "\"") && strings.HasSuffix(name, "\"") {
		return name[1 : len(name)-1]
	}
	return strings.ToUpper(name)
}
