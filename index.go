package mmdb

import (
	"fmt"
	"github.com/google/btree"
	"sync"
)

type btreeIndex struct {
	name   string
	uniq   bool
	locker sync.RWMutex
	btree  *btree.BTree
	less   Less
}

func newBtreeIndex(name string, uniq bool, less Less) *btreeIndex {
	return &btreeIndex{
		name:   name,
		uniq:   uniq,
		locker: sync.RWMutex{},
		btree:  btree.New(2),
		less:   less,
	}
}

func (i *btreeIndex) Name() string {
	return i.name
}

func (i *btreeIndex) Find(key *Item) []interface{} {
	i.locker.RLock()
	defer i.locker.RUnlock()
	item := i.btree.Get(key)
	if item == nil {
		return nil
	}
	var objs []interface{}
	for iter := item.(*Item).list().Front(); iter != nil; iter = iter.Next() {
		objs = append(objs, iter.Value)
	}
	return objs
}

func (i *btreeIndex) Insert(k *Item, obj interface{}) error {
	i.locker.Lock()
	defer i.locker.Unlock()
	item := i.btree.Get(k)
	if item != nil {
		if i.uniq {
			return fmt.Errorf("key conflict")
		}
		item.(*Item).list().PushBack(obj)
	} else {
		k.list().PushBack(obj)
		i.btree.ReplaceOrInsert(k)
	}
	return nil
}

func (i *btreeIndex) Delete(key *Item, obj interface{}) []interface{} {
	i.locker.Lock()
	defer i.locker.Unlock()
	item := i.btree.Get(key)
	if item == nil {
		return nil
	}
	l := item.(*Item).list()
	var objs []interface{}
	for iter := l.Front(); iter != nil; {
		tObj := iter.Value
		curr := iter
		iter = iter.Next()
		if tObj == obj || obj == nil {
			objs = append(objs, l.Remove(curr))
		}
	}
	return objs
}

func (i *btreeIndex) AscendRange(greaterOrEqual, lessThan *Item, iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.AscendRange(greaterOrEqual, lessThan, func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) AscendLessThan(pivot *Item, iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.AscendLessThan(pivot, func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) AscendGreaterOrEqual(pivot *Item, iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) Ascend(iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.Ascend(func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) DescendRange(lessOrEqual, greaterThan *Item, iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.DescendRange(lessOrEqual, greaterThan, func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) DescendLessOrEqual(pivot *Item, iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) DescendGreaterThan(pivot *Item, iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.DescendGreaterThan(pivot, func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) Descend(iterator ItemIterator) {
	i.locker.RLock()
	defer i.locker.RUnlock()
	i.btree.Descend(func(i btree.Item) bool {
		for iter := i.(*Item).list().Front(); iter != nil; iter = iter.Next() {
			if iterator(iter.Value) == false {
				return false
			}
		}
		return true
	})
}

func (i *btreeIndex) Min() interface{} {
	i.locker.RLock()
	defer i.locker.RUnlock()
	item := i.btree.Min()
	if item == nil {
		return nil
	}
	return item.(*Item).list().Front().Value
}

func (i *btreeIndex) Max() interface{} {
	i.locker.RLock()
	defer i.locker.RUnlock()
	item := i.btree.Max()
	if item == nil {
		return nil
	}
	return item.(*Item).list().Front().Value
}

func (i *btreeIndex) Len() int {
	i.locker.RLock()
	defer i.locker.RUnlock()
	return i.btree.Len()
}
