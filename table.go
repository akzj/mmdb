package mmdb

import (
	"fmt"
	"sync"
)

type table struct {
	name    string
	locker  sync.RWMutex
	indices []*btreeIndex
}

func newTable(name string) *table {
	return &table{
		name: name,
	}
}

func (t *table) createIndex(name string, uniq bool, less Less) error {
	t.locker.Lock()
	defer t.locker.Unlock()
	for _, index := range t.indices {
		if index.name == name {
			return fmt.Errorf("index exist")
		}
	}
	t.indices = append(t.indices, newBtreeIndex(name, uniq, less))
	return nil
}

func (t *table) Insert(obj interface{}) error {
	t.locker.RLock()
	defer t.locker.RUnlock()
	for _, index := range t.indices {
		if err := index.Insert(newItem(index.less, obj), obj); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) findIndex(name string) *btreeIndex {
	var index *btreeIndex
	t.locker.RLock()
	for _, i := range t.indices {
		if i.name == name {
			index = i
			break
		}
	}
	t.locker.RUnlock()
	return index
}

func (t *table) Index(index string, key interface{}) ([]interface{}, error) {
	btreeIndex := t.findIndex(index)
	if btreeIndex == nil {
		return nil, fmt.Errorf("no find index")
	}
	return btreeIndex.Find(newItem(btreeIndex.less, key)), nil
}

func (t *table) AscendRange(index string, greaterOrEqual, lessThan interface{}, iterator ItemIterator) error {
	btreeIndex := t.findIndex(index)
	if btreeIndex == nil {
		return fmt.Errorf("no find index")
	}
	btreeIndex.AscendRange(newItem(btreeIndex.less, greaterOrEqual),
		newItem(btreeIndex.less, lessThan), iterator)
	return nil
}
