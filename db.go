package mmdb

import (
	"bytes"
	"errors"
	"github.com/google/btree"
	"os"
	"sync"
)

var ErrNoFind = errors.New("no find")

type Transaction interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Del(key []byte) error
	Commit() error
	CommitWith(cb func(err error))
	Discard() error
}

type DB interface {
	NewTransaction(write bool) Transaction
	Update(func(tx Transaction) error) error
	View(func(tx Transaction) error) error
	Close() error
}

type Options struct {
	BtreeDegree int
	JournalDir  string
	SnapshotDir string
}

func OpenDB(Options) *DB {
	return nil
}

type db struct {
	Options
	locker  sync.RWMutex
	btree   *btree.BTree
	journal *journal
}

type transaction struct {
	readOnly *btree.BTree
	write    *btree.BTree
	db       *db
}

type entry struct {
	key   []byte
	value []byte
	del   bool
}

func (e *entry) Less(than btree.Item) bool {
	return bytes.Compare(e.key, than.(*entry).key) < 0
}

func newEntry(key, value []byte, del bool) *entry {
	return &entry{
		key:   key,
		value: value,
		del:   del,
	}
}

func (t *transaction) Get(key []byte) ([]byte, error) {
	var trees []*btree.BTree
	if t.write != nil {
		trees = append(trees, t.write)
	}
	trees = append(trees, t.readOnly)
	for _, it := range trees {
		item := it.Get(newEntry(key, nil, false))
		if item != nil {
			if e := item.(*entry); e.del {
				return nil, ErrNoFind
			} else {
				return e.value, nil
			}
		}
	}
	return nil, ErrNoFind
}

func (t *transaction) Set(key, value []byte) error {
	if t.write == nil {
		panic("readOny transaction")
	}
	t.write.ReplaceOrInsert(newEntry(key, value, false))
	return nil
}

func (t *transaction) Del(key []byte) error {
	if t.write == nil {
		panic("readOny transaction")
	}
	t.write.ReplaceOrInsert(newEntry(key, nil, true))
	return nil
}

func (t *transaction) Commit() error {
	t.db.commit()
}

func (t *transaction) CommitWith(cb func(err error)) {
	panic("implement me")
}

func (t *transaction) Discard() error {
	panic("implement me")
}

func (db *db) NewTransaction(write bool) Transaction {
	var writeBtree *btree.BTree
	if write {
		writeBtree = btree.New(db.BtreeDegree)
	}
	return &transaction{
		readOnly: db.btree.Clone(),
		write:    writeBtree,
	}
}

func (db *db) Update(callback func(tx Transaction) error) error {
	tx := db.NewTransaction(true)
	defer tx.Discard()
	if err := callback(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *db) View(callback func(tx Transaction) error) error {
	tx := db.NewTransaction(false)
	defer tx.Discard()
	return callback(tx)
}

func (db *db) Close() error {
	panic("implement me")
}

func (db *db) commit(entry2 []*entry) {

}

type journal struct {
	f *os.File
}
