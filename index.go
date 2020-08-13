package mmdb

import (
	"container/list"
	"fmt"
	"sync"
)

type Index interface {
	Name() string
	Find(key Key) []interface{}
	Insert(key Key, obj interface{}) error
	Delete(key Key, obj interface{}) []interface{}
}

type Int64Index struct {
	name   string
	uniq   bool
	locker sync.RWMutex
	objMap map[int64]interface{}
}

func (i *Int64Index) Name() string {
	return i.name
}

func (i *Int64Index) checkType(key Key) {
	if key.Type() != KeyTypeInt64 {
		panic("Int64Index no support " + key.Type().String())
	}
}

func (i *Int64Index) Find(key Key) []interface{} {
	i.checkType(key)
	i.locker.RLock()
	defer i.locker.RUnlock()
	obj, ok := i.objMap[key.Value().(int64)]
	if ok == false {
		return nil
	}
	l, ok := obj.(*list.List)
	if ok {
		var objs []interface{}
		for iter := l.Front(); iter != nil; iter = iter.Next() {
			objs = append(objs, iter.Value)
		}
		return objs
	}
	return []interface{}{obj}
}

func (i *Int64Index) Insert(key Key, obj interface{}) error {
	i.checkType(key)
	i.locker.Lock()
	defer i.locker.Unlock()
	oObj, ok := i.objMap[key.Value().(int64)]
	if !ok {
		i.objMap[key.Value().(int64)] = obj
		return nil
	}
	if i.uniq {
		return fmt.Errorf("key %d conflict", key.Value().(int64))
	}
	l, ok := oObj.(*list.List)
	if !ok {
		l = list.New()
		l.PushBack(oObj)
		i.objMap[key.Value().(int64)] = l
	}
	l.PushBack(obj)
	return nil
}

func (i *Int64Index) Delete(key Key, obj interface{}) []interface{} {
	i.checkType(key)
	i.locker.Lock()
	defer i.locker.Unlock()
	oObj, ok := i.objMap[key.Value().(int64)]
	if ok == false {
		return nil
	}
	l, ok := oObj.(*list.List)
	if ok {
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
	} else if obj == oObj {
		delete(i.objMap, key.Value().(int64))
		return []interface{}{obj}
	}
	return nil
}
