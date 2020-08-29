package mmdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/google/btree"
	"github.com/pkg/errors"

	"io"
	"os"
	"sync"
	"testing"
	"time"
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

func newIntItem(val int) *intItem {
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
	item := newIntItem(123456789)
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
		btree.ReplaceOrInsert(newIntItem(i))
	}

	for i := 0; i < 100; i++ {
		item := btree.Get(newIntItem(i))
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
			tx.ReplaceOrInsert(newIntItem(int(i)))
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
			item := tx.Get(newIntItem(int(i)))
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
			item := tx.Get(newIntItem(int(i)))
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
			item := tx.Get(newIntItem(int(i)))
			_assertTrue(int64(*item.(*intItem)) == int64(i))
		}
		return nil
	})
}

func TestDb_Conflict(t *testing.T) {
	defer func() {
		os.RemoveAll(DefaultOptions().JournalDir)
		os.RemoveAll(DefaultOptions().SnapshotDir)
	}()
	db, err := openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}))
	_assert(err)

	tx, err := db.newTransaction(true)
	_assert(err)

	tx2, err := db.newTransaction(true)
	_assert(err)

	tx.ReplaceOrInsert(newIntItem(1))
	tx2.ReplaceOrInsert(newIntItem(1))

	var wg sync.WaitGroup
	wg.Add(1)
	err = db.commit(tx, func(err error) {
		_assert(err)
		wg.Done()
	})
	_assert(err)
	wg.Wait()

	err = db.commit(tx2, func(err error) {
		fmt.Println(err)
	})
	if err == nil {
		_assert(fmt.Errorf("transaction no conflict"))
	}
}

func TestRecoveryJournal(t *testing.T) {
	defer func() {
		os.RemoveAll(DefaultOptions().JournalDir)
		os.RemoveAll(DefaultOptions().SnapshotDir)
	}()
	db, err := openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}))
	_assert(err)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			db.Update(func(tx Transaction) error {
				tx.ReplaceOrInsert(newIntItem(val))
				return nil
			})
		}(i)
	}
	wg.Wait()
	//wait for transaction commit done
	db.oracle.WaitCommittedTSMark(db.oracle.nextTS - 1)

	db.CloseWait()
	f, err := os.OpenFile("journal/100.log", os.O_RDWR, 0666)
	_assert(err)
	offset, err := f.Seek(-1, io.SeekEnd)
	_assert(err)
	fmt.Println(offset)
	_assert(f.Truncate(offset))
	_assert(f.Close())

	lastNextTS := db.oracle.nextTS
	fmt.Println(lastNextTS)
	db, err = openDB(DefaultOptions().WithRecovery(true).
		WithNew(func() Item {
			return new(intItem)
		}))
	_assert(err)
	fmt.Println(db.oracle.nextTS)
	_assertTrue(db.oracle.nextTS == lastNextTS-1)
}

func TestCleanupCommittedTX(t *testing.T) {
	defer func() {
		os.RemoveAll(DefaultOptions().JournalDir)
		os.RemoveAll(DefaultOptions().SnapshotDir)
	}()
	db, err := openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}))
	_assert(err)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			_assert(db.Update(func(tx Transaction) error {
				tx.ReplaceOrInsert(newIntItem(i))
				return nil
			}))
		}()
	}
	wg.Wait()

	db.View(func(tx Transaction) error {
		for i := 0; i < 100; i++ {
			if tx.Get(newIntItem(i)) == nil {
				t.Fatalf("no find %d", i)
			}
		}
		return nil
	})

	_assert(db.Update(func(tx Transaction) error {
		return nil
	}))
	//wait for transaction commit done
	db.oracle.WaitReadTSMark(db.oracle.nextTS - 1)
	fmt.Println("db.oracle.nextTS - 1", db.oracle.nextTS-1)
	fmt.Println("db.oracle.readMark.DoneUntil()", db.oracle.readMark.DoneUntil())
	db.cleanupBtreeWithTSs()
	db.oracle.cleanCommittedTxn()

	fmt.Println("db.oracle.committedTxns", len(db.oracle.committedTxns))
	_assertTrue(len(db.oracle.committedTxns) < 100)

	fmt.Println("read done until", db.oracle.readMark.DoneUntil())
	fmt.Println("len btreeWithTSs", len(db.btreeWithTSs))
	fmt.Println("first btreeWithTSs ts", db.btreeWithTSs[0].TS)
	_assertTrue(len(db.btreeWithTSs) < 100)
}

func TestWatermark_BeginMark(t *testing.T) {
	wm := newWatermark()

	wm.BeginMark(1)
	wm.DoneMark(1)

	wm.WaitForMark(context.Background(), 1)
}

func BenchmarkBtreeReplaceOrInsert(b *testing.B) {
	tree := btree.New(10)

	for i := 0; i < b.N; i++ {
		tree.ReplaceOrInsert(newIntItem(i))
	}
}

//chan 347662
//block-queue 569618
func Test_markDb_Update(t *testing.T) {
	defer func() {
		os.RemoveAll(DefaultOptions().JournalDir)
		os.RemoveAll(DefaultOptions().SnapshotDir)
	}()
	db, err := openDB(DefaultOptions().WithNew(func() Item {
		return new(intItem)
	}).WithSyncWrite(false))
	_assert(err)
	tx, _ := db.NewTransaction(true)
	begin := time.Now()
	count := 1000000
	for i := 0; i < count; i++ {
		tx.ReplaceOrInsert(newIntItem(i))
		if i%10 == 0 {
			_assert(tx.Commit())
			tx, _ = db.NewTransaction(true)
		}
	}
	_assert(tx.Commit())
	fmt.Println("write items/second", float64(count)/time.Now().Sub(begin).Seconds())

	begin = time.Now()
	db.View(func(tx Transaction) error {
		for i := 0; i < count; i++ {
			if tx.Get(newIntItem(i)) == nil {
				t.Fatalf("get %d failed", i)
			}
		}
		return nil
	})
	fmt.Println("read items/second", int64(float64(count)/time.Now().Sub(begin).Seconds()))
}

func TestBadgerDB(t *testing.T) {
	defer func() {
		os.RemoveAll("badger")
	}()
	db, err := badger.Open(badger.DefaultOptions("badger").WithSyncWrites(false))
	_assert(err)

	tx := db.NewTransaction(true)

	begin := time.Now()
	count := 1000000
	for i := 0; i < count; i++ {
		var buffer bytes.Buffer
		binary.Write(&buffer, binary.BigEndian, int64(i))
		_assert(tx.Set(buffer.Bytes(), buffer.Bytes()))
		if i%10 == 0 {
			_assert(tx.Commit())
			tx = db.NewTransaction(true)
		}
	}
	_assert(tx.Commit())
	fmt.Println("write items/second", float64(count)/time.Now().Sub(begin).Seconds())
	begin = time.Now()
	db.View(func(txn *badger.Txn) error {
		for i := 0; i < count; i++ {
			var buffer bytes.Buffer
			binary.Write(&buffer, binary.BigEndian, int64(i))
			if _, err := txn.Get(buffer.Bytes()); err != nil {
				t.Fatalf(err.Error())
			}
		}
		return nil
	})
	fmt.Println("read items/second", int64(float64(count)/time.Now().Sub(begin).Seconds()))
}
