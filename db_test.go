package mmdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"os"
	"sync"
	"testing"
)

/*func TestName(t *testing.T) {
	tree := btree.New(10)
	var writes int64
	var lastWrites int64

	go func() {
		for count := 0; count < 1000000; count++ {
			if count%10 == 0 {
				tree = tree.Clone()
			}
			tree.ReplaceOrInsert(btree.Int(count))
			atomic.AddInt64(&writes, 1)
		}
	}()
	for {
		time.Sleep(time.Second)
		w := atomic.LoadInt64(&writes)
		fmt.Println((w - lastWrites) / 10000)
		lastWrites = w
	}
}*/

type intItem int

func newInt64Item(val int) *intItem {
	return (*intItem)(&val)
}

func (i *intItem) Less(other btree.Item) bool {
	return *i < *other.(*intItem)
}

func (i *intItem) MarshalBinary() (data []byte, err error) {
	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.BigEndian, int64(*i)); err != nil {
		return nil, errors.WithStack(err)
	}
	return buffer.Bytes(), nil
}

func (i *intItem) UnmarshalBinary(data []byte) error {
	var _int64 int64
	if err := binary.Read(bytes.NewReader(data), binary.BigEndian, &_int64); err != nil {
		return errors.WithStack(err)
	}
	*i = intItem(_int64)
	return nil
}

func TestInt64Item(t *testing.T) {
	item := newInt64Item(123456789)
	data, err := item.MarshalBinary()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Printf("%d", len(data))
	var item2 intItem
	item2.UnmarshalBinary(data)
	_assertTrue(*item == item2)

	btree := btree.New(3)

	for i := 0; i < 100; i++ {
		btree.ReplaceOrInsert(newInt64Item(i))
	}

	for i := 0; i < 100; i++ {
		item := btree.Get(newInt64Item(i))
		_assertTrue(int(*item.(*intItem)) == int(i))
	}
}

func TestOpenDB(t *testing.T) {
	defer func() {
		os.RemoveAll(DefaultOptions().JournalDir)
		os.RemoveAll(DefaultOptions().SnapshotDir)
	}()
	db, err := openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}))
	_assert(err)
	err = db.Update(func(tx Transaction) error {
		for i := 0; i < 100; i++ {
			tx.ReplaceOrInsert(newInt64Item(int(i)))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println("db.oracle.readMark.doneUntil", db.oracle.readMark.doneUntil)
	fmt.Println("db.oracle.nextTS", db.oracle.nextTS)

	err = db.View(func(tx Transaction) error {
		fmt.Println("readTS", tx.(*transaction).readTs)
		for i := 0; i < 100; i++ {
			item := tx.Get(newInt64Item(int(i)))
			_assertTrue(int(*item.(*intItem)) == int(i))
		}
		return nil
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	db.CloseWait()

	//reload journal
	db, err = openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}))

	err = db.View(func(tx Transaction) error {
		for i := 0; i < 100; i++ {
			item := tx.Get(newInt64Item(int(i)))
			_assertTrue(int64(*item.(*intItem)) == int64(i))
		}
		return nil
	})

	//make snapshot
	var wg sync.WaitGroup
	wg.Add(1)
	db.journalCompaction(func() {
		wg.Done()
	})

	wg.Wait()

	db.CloseWait()

	_assert(os.RemoveAll(db.JournalDir))

	db, err = openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}))
	_assert(err)

	err = db.View(func(tx Transaction) error {
		for i := 0; i < 100; i++ {
			item := tx.Get(newInt64Item(int(i)))
			_assertTrue(int64(*item.(*intItem)) == int64(i))
		}
		return nil
	})

}

func TestWatermark_BeginMark(t *testing.T) {
	wm := newWatermark()

	wm.BeginMark(1)
	wm.DoneMark(1)

	wm.WaitForMark(context.Background(), 1)
}
