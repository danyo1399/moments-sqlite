package momentssqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/danyo1399/moments"
	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	EventDeserialiser moments.EventDeserialiser
	DataPath          string
}
type StoreProvider struct {
	config Config
	dbs    map[string]*sql.DB
	mu     sync.Mutex
}

func NewStoreProvider(config Config) *StoreProvider {
	dbs := make(map[string]*sql.DB)
	return &StoreProvider{config, dbs, sync.Mutex{}}
}

func (s *StoreProvider) getDb(tenant string) (*sql.DB, error) {
	db, ok := s.dbs[tenant]
	if !ok {
		dbpath := s.tenantPath(tenant)
		exists, _ := fileExists(dbpath)
		if !exists {
			return nil, fmt.Errorf("tenant %s does not exist. path %s", tenant, dbpath)
		}
		db, err := sql.Open("sqlite3", dbpath)
		if err != nil {
			return nil, err
		}
		s.dbs[tenant] = db
	}
	return db, nil
}

func (s *StoreProvider) tenantPath(tenant string) string {
	fPath := filepath.Join(s.config.DataPath, tenant+".db")
	fullPath, err := filepath.Abs(fPath)
	if err != nil {
		panic(err)
	}
	return fullPath
}

func (s *StoreProvider) NewTenant(tenantId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dbPath := s.tenantPath(tenantId)
	tempDbPath := dbPath + ".tmp"
	exists, err := fileExists(dbPath)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("tenant %s already exists", tenantId)
	}
	tempConStr := fmt.Sprintf("file:%s?_journal=WAL&_txlock=immediate&_sync=NORMAL", tempDbPath)
	conStr := fmt.Sprintf("file:%s", dbPath)
	db, err := sql.Open("sqlite3", tempConStr)
	if err != nil {
		return err
	}
	err = Migrate(db)
	if err != nil {
		db.Close()
		os.Remove(tempDbPath)
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}
	err = os.Rename(tempDbPath, dbPath)
	if err != nil {
		return err
	}
	db, err = sql.Open("sqlite3", conStr)
	if err != nil {
		return err
	}
	s.dbs[tenantId] = db
	return nil
}

func (s *StoreProvider) DeleteTenant(tenantId string) error {
	db := s.dbs[tenantId]
	if db == nil {
		return fmt.Errorf("tenant %s does not exist", tenantId)
	}
	db.Close()
	delete(s.dbs, tenantId)
	os.Remove(s.tenantPath(tenantId))

	return nil
}

func (s *StoreProvider) GetStore(tenant string) (moments.Store, error) {
	db, err := s.getDb(tenant)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(db, s.config)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (s *StoreProvider) Close() {
}
