package mmdb

import (
	"fmt"
	"testing"
)

func TestNewTable(t *testing.T) {

	type User struct {
		ID   int64 //uniq
		Name string
	}
	table := newTable("A")
	table.createIndex("ID", true, func(left interface{}, right interface{}) bool {
		return left.(*User).ID < right.(*User).ID
	})
	table.createIndex("name", false, func(left interface{}, right interface{}) bool {
		return left.(*User).Name < right.(*User).Name
	})

	for i := int64(0); i < 100; i++ {
		err := table.Insert(&User{
			ID:   i,
			Name: fmt.Sprintf("akzj:%d", i/10),
		})
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	objs, err := table.Index("ID", &User{ID: 10})
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, obj := range objs {
		u := obj.(*User)
		if u.ID != 10 {
			t.Fatal(u)
		}
	}

	objs, err = table.Index("name", &User{Name: "akzj:3"})
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(objs) != 10 {
		t.Fatal("index error")
	}

	var IDCount int
	table.AscendRange("ID", &User{ID: 5}, &User{ID: 10}, func(obj interface{}) bool {
		IDCount++
		return true
	})
	if IDCount != 5 {
		t.Fatal(IDCount)
	}

	var NameCount int
	table.AscendRange("name", &User{Name: "akzj:2"}, &User{Name: "akzj:4"}, func(obj interface{}) bool {
		NameCount++
		return true
	})
	if NameCount != 20 {
		t.Fatal(NameCount)
	}

}
