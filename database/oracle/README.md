# Oracle

This driver adds support for running migrations against Oracle 19c and newer
instances using the [go-ora](https://github.com/sijms/go-ora) driver. The
connection URL has the following form:

```
oracle://user:password@host:port/service_name?param=value
```

## Configuration

The driver accepts the following custom query parameters:

- `x-migrations-table`: overrides the default migrations table name
  (`schema_migrations`).
- `x-schema`: explicitly sets the Oracle schema to inspect for migrations.
  When omitted the current schema of the connected user is used.
- `x-multi-statement`: enables parsing and executing multiple SQL statements
  from a single migration using `;` as the delimiter.
- `x-multi-statement-max-size`: overrides the maximum size (bytes) accepted
  for a multi-statement migration. The default is 10 MiB.

All other query parameters are forwarded to the underlying go-ora connection
string builder. By default the driver enables a 60 second connection timeout
and prefetches 25 rows per round trip, mirroring the upstream
[`gogf/gf`](https://github.com/gogf/gf) Oracle driver defaults. These values can
be overridden by supplying query parameters like
`?CONNECTION+TIMEOUT=30&PREFETCH_ROWS=100`.

The driver relies on `DBMS_LOCK` for advisory locking. The database user must
therefore have `EXECUTE` permissions on `DBMS_LOCK`.

## Testing

The integration tests use the `gvenzl/oracle-xe:21-slim` container image. To run
them locally ensure Docker is available and execute:

```
make test DATABASE_TEST='oracle'
```

The tests will create an application user named `migrate` with access to the
`XEPDB1` service.
