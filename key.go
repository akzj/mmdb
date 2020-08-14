package mmdb

import (
	"container/list"
	"github.com/google/btree"
)

type Less func(left interface{}, right interface{}) bool
type ItemIterator func(obj interface{}) bool
type Item struct {
	less Less
	obj  interface{}
	l    *list.List
}

func newItem(less Less, obj interface{}) *Item {
	return &Item{
		less: less,
		obj:  obj,
	}
}
func (i *Item) list() *list.List {
	if i.l == nil {
		i.l = list.New()
	}
	return i.l
}

func (i *Item) Less(item btree.Item) bool {
	return i.less(i.obj, item.(*Item).obj)
}
