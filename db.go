package mmdb

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"github.com/google/btree"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

var ErrKeyNoFind = errors.New("key no find")
var ErrConflict = errors.New("transaction conflict")
var ErrDBClose = errors.New("db close error")

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
	locker         sync.RWMutex
	btree          *btree.BTree
	btreeLocker    sync.Mutex
	oracle         *oracle
	journal        *journal
	closeWG        sync.WaitGroup
	isClose        int32
	commitRequests chan commitRequest
}

type transaction struct {
	db       *db
	readOnly *btree.BTree
	write    *btree.BTree

	isDiscard   bool
	readTs      int64
	committedTS int64
	sync        bool
}

const metaDelete = 1
const entryType = 1
const commitEntryType = 2

type commitEntry struct {
	TS int64
}

type entry struct {
	key   []byte
	value []byte
	meta  byte
}

func (e *entry) Less(than btree.Item) bool {
	return bytes.Compare(e.key, than.(*entry).key) < 0
}

func (commitEntry *commitEntry) encode(writer io.Writer) {
	binary.Write(writer, binary.BigEndian, commitEntryType)
	binary.Write(writer, binary.BigEndian, commitEntry.TS)
}

func (e *entry) encode(writer io.Writer) {
	binary.Write(writer, binary.BigEndian, entryType)
	binary.Write(writer, binary.BigEndian, int32(len(e.key)))
	binary.Write(writer, binary.BigEndian, int32(len(e.value)))
	writer.Write(e.key)
	writer.Write(e.value)
}

func newEntry(key, value []byte, meta byte) *entry {
	return &entry{
		key:   key,
		value: value,
		meta:  meta,
	}
}

func (t *transaction) Get(key []byte) ([]byte, error) {
	var trees []*btree.BTree
	if t.write != nil {
		trees = append(trees, t.write)
	}
	trees = append(trees, t.readOnly)
	for _, it := range trees {
		item := it.Get(newEntry(key, nil, 0))
		if item != nil {
			if e := item.(*entry); e.meta == metaDelete {
				return nil, ErrKeyNoFind
			} else {
				return e.value, nil
			}
		}
	}
	return nil, ErrKeyNoFind
}


func (t *transaction) Set(key, value []byte) error {
	if t.write == nil {
		panic("readOny transaction")
	}
	t.write.ReplaceOrInsert(newEntry(key, value, 0))
	return nil
}

func (t *transaction) Del(key []byte) error {
	if t.write == nil {
		panic("readOny transaction")
	}
	t.write.ReplaceOrInsert(newEntry(key, nil, metaDelete))
	return nil
}

func (t *transaction) Commit() error {
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	if err := t.db.commit(t, func(e error) {
		err = e
		wg.Done()
	}); err != nil {
		return err
	}
	wg.Wait()
	return err
}

func (t *transaction) CommitWith(cb func(err error)) {
	if err := t.db.commit(t, cb); err != nil {
		cb(err)
	}
}

func (t *transaction) Discard() error {
	if t.isDiscard {
		return nil
	}
	t.isDiscard = true
	t.db.oracle.readMark.DoneMark(t.readTs)
	return nil
}

func (db *db) checkClose() bool {
	return atomic.LoadInt32(&db.isClose) == 1
}

func (db *db) NewTransaction(update bool) (Transaction, error) {
	if db.checkClose() {
		return nil, ErrDBClose
	}
	var tree *btree.BTree
	if update {
		tree = btree.New(db.BtreeDegree)
	}
	return &transaction{
		db:       db,
		readOnly: db.btree.Clone(),
		write:    tree,
		readTs:   db.oracle.getReadTS(),
	}, nil
}

func (db *db) Update(callback func(tx Transaction) error) error {
	tx, err := db.NewTransaction(true)
	if err != nil {
		return err
	}
	defer tx.Discard()
	if err := callback(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *db) View(callback func(tx Transaction) error) error {
	tx, err := db.NewTransaction(false)
	if err != nil {
		return err
	}
	defer tx.Discard()
	return callback(tx)
}

func (db *db) Close() error {
	if atomic.CompareAndSwapInt32(&db.isClose, 0, 1) == false {
		return nil
	}
	//wait for committed update flush to log entry done
	db.oracle.readMark.DoneMark(db.oracle.getReadTS())
	db.commitRequests <- commitRequest{close: true}
	return nil
}

func (db *db) commit(tx *transaction, cb func(err error)) error {
	ts := tx.db.oracle.newCommittedTs(tx)
	if ts == 0 {
		return ErrConflict
	}
	tx.committedTS = ts

	var buffer bytes.Buffer
	tx.write.Descend(func(item btree.Item) bool {
		e := item.(*entry)
		e.encode(&buffer)
		return true
	})
	entry := commitEntry{TS: ts}
	entry.encode(&buffer)
	db.commitRequests <- commitRequest{
		journalBuf: &buffer,
		update:     tx.write,
		sync:       tx.sync,
		commitTS:   tx.committedTS,
		callback:   cb,
	}
	return nil
}
func _assert(err error) {
	if err != nil {
		panic(err)
	}
}

func (db *db) cloneBtree() *btree.BTree {
	db.btreeLocker.Lock()
	defer db.btreeLocker.Unlock()
	return db.btree.Clone()
}

func (db *db) updateBtree(tree *btree.BTree) {
	db.btreeLocker.Lock()
	defer db.btreeLocker.Unlock()
	db.btree = tree
}

func (db *db) doWriteCommitRequests(requests []commitRequest) {
	for _, request := range requests {
		_assert(db.journal.append(request.journalBuf))
		if request.sync {
			_assert(db.journal.sync())
		}
		cBTree := db.cloneBtree()
		request.update.Descend(func(item btree.Item) bool {
			if item.(*entry).meta == metaDelete {
				cBTree.Delete(item)
			} else {
				cBTree.ReplaceOrInsert(item)
			}
			return true
		})
		db.updateBtree(cBTree)
		db.oracle.committedDone(request.commitTS)
		request.callback(nil)
	}
}

func (db *db) writeCommitRequests() {
	defer func() {
		db.closeWG.Done()
	}()
	var commitRequests []commitRequest
	var isClose bool
	appendRequest := func(request commitRequest) {
		if request.close {
			isClose = true
			return
		}
		commitRequests = append(commitRequests, request)
	}
	for isClose == false {
		select {
		case request := <-db.commitRequests:
			appendRequest(request)
			for {
				select {
				case request := <-db.commitRequests:
					appendRequest(request)
				default:
					db.doWriteCommitRequests(commitRequests)
					commitRequests = commitRequests[:0]
				}
			}
		}
	}
}

type journal struct {
	f *os.File
}

func (j *journal) append(reader io.Reader) error {
	_, err := io.Copy(j.f, reader)
	return err
}

func (j *journal) sync() error {
	return j.f.Sync()
}

type committedTxn struct {
	ts           int64
	conflictKeys *btree.BTree
}

type oracle struct {
	locker sync.Mutex

	committedMark *watermark
	readMark      *watermark
	doneTS        int64
	nextTS        int64
	committedTxns []committedTxn
}

func (o *oracle) checkConflict(tx *transaction) bool {
	for _, it := range o.committedTxns {
		if it.ts < tx.readTs {
			continue
		}
		var conflict bool
		tx.write.Ascend(func(i btree.Item) bool {
			if it.conflictKeys.Get(i) != nil {
				conflict = true
				return false
			}
			return true
		})
		if conflict {
			return true
		}
	}
	return false
}

func (o *oracle) newCommittedTs(tx *transaction) int64 {
	o.locker.Lock()
	defer o.locker.Unlock()
	if o.checkConflict(tx) {
		return 0
	}

	ts := o.nextTS
	o.nextTS++
	o.committedMark.BeginMark(ts)
	o.readMark.DoneMark(tx.readTs)
	o.committedTxns = append(o.committedTxns, committedTxn{
		ts:           ts,
		conflictKeys: tx.write,
	})
	return ts
}

func (o *oracle) committedDone(ts int64) {
	o.committedMark.DoneMark(ts)
}

func (o *oracle) getReadTS() int64 {
	o.locker.Lock()
	readTs := o.nextTS - 1
	o.locker.Unlock()
	o.readMark.BeginMark(readTs)

	//wait for committed index done
	_assert(o.committedMark.WaitForMark(context.Background(), readTs))
	return readTs
}

type commitRequest struct {
	journalBuf *bytes.Buffer
	update     *btree.BTree
	sync       bool
	commitTS   int64
	close      bool
	callback   func(err error)
}

type mark struct {
	index  int64
	done   bool
	waiter chan interface{}
}

// An Int64Heap is a min-heap of ints.
type Int64Heap []int64

func (h Int64Heap) Len() int           { return len(h) }
func (h Int64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h Int64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Int64Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int64))
}

func (h *Int64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type watermark struct {
	locker       sync.Locker
	marks        chan mark
	pendingCount map[int64]int
	doneUntil    int64
	waiters      map[int64][]chan interface{}
	int64Heap    *Int64Heap
}

func (wm *watermark) BeginMark(index int64) {
	wm.marks <- mark{
		index: index,
		done:  false,
	}
}

func (wm *watermark) DoneMark(index int64) {
	wm.marks <- mark{
		index: index,
		done:  false,
	}
}

func (wm *watermark) WaitForMark(ctx context.Context, index int64) error {
	if index <= wm.DoneUntil() {
		return nil
	}
	var ch = make(chan interface{})
	wm.marks <- mark{
		index:  index,
		waiter: ch,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
	}
	return nil
}

func (wm *watermark) processMark(m mark) {
	count, ok := wm.pendingCount[m.index]
	if ok == false {
		heap.Push(wm.int64Heap, m.index)
	}
	count += 1
	if m.done {
		count -= 2
	}
	wm.pendingCount[m.index] = count

	var doneUntil = wm.DoneUntil()
	for len(*wm.int64Heap) != 0 {
		min := (*wm.int64Heap)[0]
		if count, _ := wm.pendingCount[min]; count > 0 {
			break
		}
		heap.Pop(wm.int64Heap)
		delete(wm.pendingCount, min)
		doneUntil = min
	}

	if doneUntil == wm.DoneUntil() {
		return
	}
	atomic.StoreInt64(&wm.doneUntil, doneUntil)
	wm.notify()
}

func (wm *watermark) notify() {
	doneUntil := wm.DoneUntil()
	for index, waiters := range wm.waiters {
		if index <= doneUntil {
			for _, waiter := range waiters {
				close(waiter)
			}
			delete(wm.waiters, index)
		}
	}
}

func (wm *watermark) process() {
	for {
		select {
		case m := <-wm.marks:
			if m.waiter != nil {
				if wm.DoneUntil() > m.index {
					close(m.waiter)
					continue
				}
				wm.waiters[m.index] = append(wm.waiters[m.index], m.waiter)
			} else {
				wm.processMark(m)
			}
		}
	}
}

func (wm *watermark) DoneUntil() int64 {
	return atomic.LoadInt64(&wm.doneUntil)
}

