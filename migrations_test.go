package momentssqlite

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func prepareDb(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	err = EnsureMigrationsTable(db)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func Test_GivenNewDatabase_WhenGetNextMigration_ThenReturnsNextMigrationToRun(t *testing.T) {
	db := prepareDb(t)
	migrations, err := LoadMigrations()
	assert.NoError(t, err)

	migration1, err := GetNextMigration(db, migrations)
	assert.NoError(t, err)

	err = PerformMigration(db, migration1)
	assert.NoError(t, err)

	migration2, err := GetNextMigration(db, migrations)
	assert.NoError(t, err)

	err = PerformMigration(db, migration2)
	assert.NoError(t, err)

	migration3, err := GetNextMigration(db, migrations)
	assert.NoError(t, err)

	assert.NotEqual(t, migration1, Migration{})
	assert.Equal(t, "20250601_01_create_migration_table", migration1.Name)
	assert.Contains(t, migration1.Sql, "create unique index idx_migrations")

	assert.NotEqual(t, migration2, Migration{})
	assert.Equal(t, "20250601_02_add_tables", migration2.Name)
	assert.Contains(t, migration2.Sql, "create table streams")

	assert.Equal(t, migration3, Migration{})
}

func Test_GivenMultipleMigrationsExist_WhenLoadMigrations_ThenLoadsMigrationsFromDisk(t *testing.T) {
	migrations, err := LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	assert.Len(t, migrations, 2)
	assert.Equal(t, "20250601_01_create_migration_table", migrations[0].Name)
	assert.Equal(t, "20250601_02_add_tables", migrations[1].Name)
	assert.Contains(t, migrations[0].Sql, "create unique index idx_migrations")
	assert.Contains(t, migrations[1].Sql, "create table streams")
	for _, migration := range migrations {
		t.Log(migration.Name)
	}
}
