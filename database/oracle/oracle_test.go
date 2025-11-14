package oracle

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/docker/docker/api/types/mount"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	gora "github.com/sijms/go-ora/v2"
)

const (
	testDefaultPort = 1521
	appUser         = "migrate"
	appPassword     = "migrate"
	serviceName     = "XEPDB1"
)

var (
	oracleOptions dktest.Options
	specs         []dktesting.ContainerSpec
)

func init() {
	scriptDir, err := filepath.Abs("database/oracle/testdata")
	if err != nil {
		panic(err)
	}

	oracleOptions = dktest.Options{
		Env: map[string]string{
			"ORACLE_PASSWORD":   "Password123",
			"APP_USER":          appUser,
			"APP_USER_PASSWORD": appPassword,
		},
		PortRequired:   true,
		ReadyFunc:      isReady,
		PullTimeout:    15 * time.Minute,
		Timeout:        15 * time.Minute,
		ReadyTimeout:   30 * time.Second,
		CleanupTimeout: 5 * time.Minute,
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: scriptDir,
				Target: "/container-entrypoint-initdb.d",
			},
		},
	}

	specs = []dktesting.ContainerSpec{
		{ImageName: "gvenzl/oracle-xe:21-slim", Options: oracleOptions},
	}
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(testDefaultPort)
	if err != nil {
		return false
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		return false
	}

	dsn := gora.BuildUrl(ip, portNum, serviceName, appUser, appPassword, nil)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err := db.PingContext(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		if err == sqldriver.ErrBadConn {
			return false
		}
		log.Println("ping error:", err)
		return false
	}

	return true
}

func SkipIfUnsupportedArch(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		t.Skipf("Oracle image is not supported on arch %s", runtime.GOARCH)
	}
}

func oracleConnectionString(host, port string) string {
	return fmt.Sprintf("oracle://%s:%s@%s:%s/%s", appUser, appPassword, host, port, serviceName)
}

func Test(t *testing.T) {
	t.Run("test", testBasic)
	t.Run("testMigrate", testMigrate)
	t.Run("testMultiStatement", testMultiStatement)
	t.Run("testErrorParsing", testErrorParsing)
	t.Run("testLockWorks", testLockWorks)
	t.Run("testFilterCustomQuery", testFilterCustomQuery)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing", spec.ImageName, "error:", err)
			}
		}
	})
}

func testBasic(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t)

		ip, port, err := c.Port(testDefaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port)
		drv := &Oracle{}
		d, err := drv.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1 FROM dual"))

		if err := d.(*Oracle).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		if err := d.(*Oracle).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t)

		ip, port, err := c.Port(testDefaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port)
		drv := &Oracle{}
		d, err := drv.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", serviceName, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

		if err := d.(*Oracle).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func testMultiStatement(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t)

		ip, port, err := c.Port(testDefaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port) + "?x-multi-statement=true"
		drv := &Oracle{}
		d, err := drv.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := d.Run(strings.NewReader("CREATE TABLE foo (id NUMBER(10)); CREATE TABLE bar (id NUMBER(10));")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		var exists int
		query := `SELECT COUNT(*) FROM user_tables WHERE table_name IN ('FOO', 'BAR')`
		if err := d.(*Oracle).conn.QueryRowContext(context.Background(), query).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != 2 {
			t.Fatalf("expected both tables to exist")
		}
	})
}

func testErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t)

		ip, port, err := c.Port(testDefaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port)
		drv := &Oracle{}
		d, err := drv.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		err = d.Run(strings.NewReader("CREATE TABLE"))
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}

func testLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t)

		ip, port, err := c.Port(testDefaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port)
		drv := &Oracle{}
		d, err := drv.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := d.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := d.Lock(); !errors.Is(err, database.ErrLocked) {
			t.Fatalf("expected ErrLocked, got %v", err)
		}
		if err := d.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func testFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t)

		ip, port, err := c.Port(testDefaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port) + "?foo=bar&x-migrations-table=custom_schema"
		drv := &Oracle{}
		d, err := drv.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		if d.(*Oracle).config.migrationsTableName != "CUSTOM_SCHEMA" {
			t.Fatalf("expected custom migrations table to be set")
		}
	})
}
