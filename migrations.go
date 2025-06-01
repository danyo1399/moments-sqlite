package momentssqlite

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/danyo1399/gotils"
)

type Migration struct {
	Name string
	Sql  string
}

func PerformMigration(db *sql.DB, migration Migration) error {
	tran, err := db.Begin()
	if err != nil {
		return err
	}
	defer tran.Rollback()
	_, err = tran.Exec("insert into migrations(name) values(?)", migration.Name)
	if err != nil {
		return err
	}
	_, err = tran.Exec(migration.Sql)
	if err != nil {
		return err
	}
	err = tran.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func LoadMigrations() ([]Migration, error) {
	dir := "./migrations"
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files = gotils.FilterSlice(files, func(f os.DirEntry) bool {
		return !f.IsDir() && strings.HasSuffix(f.Name(), ".sql")
	})
	migrations := make([]Migration, 0)

	for _, file := range files {
		filePath := dir + "/" + file.Name()
		contents, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		migration := Migration{
			Name: file.Name()[:len(file.Name())-4],
			Sql:  string(contents),
		}
		migrations = append(migrations, migration)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Name < migrations[j].Name
	})
	return migrations, nil
}

func GetNextMigration(db *sql.DB, migrations []Migration) (Migration, error) {
	if len(migrations) == 0 {
		return Migration{}, nil
	}
	rows, err := db.Query("select name from migrations order by id")
	if err != nil {
		return Migration{}, err
	}
	defer rows.Close()
	hasRow := rows.Next()
	if !hasRow {
		return migrations[0], nil
	}
	for _, migration := range migrations {
		if !hasRow {
			return migration, nil
		}
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return Migration{}, err
		}
		if migration.Name != name {
			return Migration{}, fmt.Errorf("migration name %s does not match %s", migration.Name, name)
		}
		hasRow = rows.Next()
	}
	return Migration{}, nil
}

func EnsureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
	create table if not exists migrations (
    id integer primary key autoincrement,
    name text not null,
    created_at timestamp not null default current_timestamp
  );
`)
	return err
}

func Migrate(db *sql.DB) error {
	err := EnsureMigrationsTable(db)
	if err != nil {
		return err
	}
	migrations, err := LoadMigrations()
	if err != nil {
		return err
	}
	for {
		migration, err := GetNextMigration(db, migrations)
		if err != nil {
			return err
		}
		if migration == (Migration{}) {
			break
		}
		err = PerformMigration(db, migration)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return nil
}
