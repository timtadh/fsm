package store

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

import (
	_ "github.com/mattn/go-sqlite3"
)

import (
	"github.com/timtadh/goiso"
)


type SqliteStore struct {
	g *goiso.Graph
	path string
	conn *sql.DB
	add *sql.Stmt
	find *sql.Stmt
	keys *sql.Stmt
	size *sql.Stmt
}

func NewSqlite(g *goiso.Graph, path string) (*SqliteStore, error) {
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	s := &SqliteStore{
		g: g,
		path: path,
		conn: conn,
	}
	err = s.CreateTables()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SqliteStore) CreateTables() error {
	_, err := s.conn.Exec(`PRAGMA synchronous = OFF;`)
	if err != nil {
		return err
	}
	_, err = s.conn.Exec(`PRAGMA journal_mode = MEMORY;`)
	if err != nil {
		return err
	}
	_, err = s.conn.Exec(`
		CREATE TABLE subgraphs (
			label BLOB NOT NULL,
			subgraph BLOB NOT NULL
		);
		CREATE INDEX label_idx ON subgraphs (
			label
		);
		CREATE UNIQUE INDEX label_subgraph_idx ON subgraphs (
			label, subgraph
		);
	`)
	return err
}

func (s *SqliteStore) Delete() {
	err := s.conn.Close()
	if err != nil {
		log.Panic(err)
	}
	err = os.Remove(s.path)
	if err != nil {
		log.Panic(err)
	}
}

func (s *SqliteStore) sizeStmt() (*sql.Stmt, error) {
	if s.size != nil {
		return s.size, nil
	}
	stmt, err := s.conn.Prepare(`
		SELECT count(*)
		FROM (
			SELECT DISTINCT label
			FROM subgraphs
		) as inner;
	`)
	if err != nil {
		return nil, err
	}
	s.size = stmt
	return stmt, nil
}

func (s *SqliteStore) Size() (count int) {
	stmt, err := s.sizeStmt()
	if err != nil {
		log.Panic(err)
	}
	err = stmt.QueryRow().Scan(&count)
	if err != nil {
		log.Panic(err)
	}
	return count
}

func (s *SqliteStore) keysStmt() (*sql.Stmt, error) {
	if s.keys != nil {
		return s.keys, nil
	}
	stmt, err := s.conn.Prepare(`
		SELECT DISTINCT label
		FROM subgraphs;
	`)
	if err != nil {
		return nil, err
	}
	s.keys = stmt
	return stmt, nil
}

func (s *SqliteStore) Keys() (it BytesIterator) {
	stmt, err := s.keysStmt()
	if err != nil {
		log.Panic(err)
	}
	rows, err := stmt.Query()
	if err != nil {
		log.Panic(err)
	}
	it = func() (k []byte, _ BytesIterator) {
		if !rows.Next() {
			err := rows.Err()
			if err != nil {
				log.Panic(err)
			}
			rows.Close()
			return nil, nil
		}
		err := rows.Scan(&k)
		if err != nil {
			log.Panic(err)
		}
		return k, it
	}
	return it
}

func (s *SqliteStore) Values() SGIterator {
	panic("not-implemented")
}

func (s *SqliteStore) Iterate() Iterator {
	return s.kvIter(s.conn.Prepare(`
		SELECT label, subgraph
		FROM subgraphs
	`))
}

func (s *SqliteStore) Backward() Iterator {
	return s.kvIter(s.conn.Prepare(`
		SELECT label, subgraph
		FROM subgraphs
		ORDER BY label DESC;
	`))
}

func (s *SqliteStore) kvIter(stmt *sql.Stmt, err error) (it Iterator) {
	if err != nil {
		log.Panic(err)
	}
	rows, err := stmt.Query()
	if err != nil {
		log.Panic(err)
	}
	return s.rowsIter(rows)
}

func (s *SqliteStore) rowsIter(rows *sql.Rows) (it Iterator) {
	it = func() ([]byte, *goiso.SubGraph, Iterator) {
		var key []byte
		var bytes []byte
		var err error
		if !rows.Next() {
			err := rows.Err()
			if err != nil {
				log.Panic(err)
			}
			rows.Close()
			return nil, nil, nil
		}
		err = rows.Scan(&key, &bytes)
		if err != nil {
			log.Panic(err)
		}
		sg := goiso.DeserializeSubGraph(s.g, bytes, key)
		return key, sg, it
	}
	return it
}

func (s *SqliteStore) Has(key []byte) bool {
	return s.Count(key) > 0
}

func (s *SqliteStore) Count(key []byte) int {
	count := 0
	for _, _, next := s.Find(key)(); next != nil ; _, _, next = next() {
		count++
	}
	return count
}

func (s *SqliteStore) addStmt() (*sql.Stmt, error) {
	if s.add != nil {
		return s.add, nil
	}
	stmt, err := s.conn.Prepare(`
		INSERT OR IGNORE INTO subgraphs (label, subgraph)
		VALUES (?, ?);
	`)
	if err != nil {
		return nil, err
	}
	s.add = stmt
	return stmt, nil
}

func (s *SqliteStore) Add(key []byte, sg *goiso.SubGraph) {
	stmt, err := s.addStmt()
	if err != nil {
		log.Panic(err)
	}
	value := sg.Serialize()
	if len(value) < 0 {
		log.Panic(fmt.Errorf("Could not serialize sg, %v\n%v\n%v", len(value), sg, value))
	}
	_, err = stmt.Exec(key, value)
	if err != nil {
		log.Panic(err)
	}
}

func (s *SqliteStore) findStmt() (*sql.Stmt, error) {
	if s.find != nil {
		return s.find, nil
	}
	stmt, err := s.conn.Prepare(`
		SELECT label, subgraph
		FROM subgraphs
		WHERE label = ?;
	`)
	if err != nil {
		return nil, err
	}
	s.find = stmt
	return stmt, nil
}

func (s *SqliteStore) Find(key []byte) Iterator {
	stmt, err := s.findStmt()
	if err != nil {
		log.Panic(err)
	}
	rows, err := stmt.Query(key)
	if err != nil {
		log.Panic(err)
	}
	return s.rowsIter(rows)
}

func (s *SqliteStore) Remove(key []byte, where func(*goiso.SubGraph) bool) error {
	panic("will not implement")
}

