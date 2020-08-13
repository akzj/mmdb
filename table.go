package mmdb

import (
	"github.com/fatih/structs"
	"reflect"
	"sync"
)

type table struct {
	name           string
	indexMapLocker sync.RWMutex
	primaryKey     string
	indexMap       []Index
}

func newTable(name string) *table {
	return &table{
		name: name,
	}
}

func (t *table) insert(obj interface{}) error {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Ptr {
		// if pointer get the underlying element≤
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("not struct")
	}
	t.indexMapLocker.RLock()
	defer t.indexMapLocker.RUnlock()
	_ = structs.New(obj)
	for _, index := range t.indexMap {
		value := v.FieldByName(index.Name())
		if err := index.Insert(newKey(value.Interface()), obj); err != nil {
			return err
		}
	}
	return nil
}
