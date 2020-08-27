package mmdb

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/akzj/block-queue"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrNoFind = errors.New("no find")
var ErrDBClose = errors.New("db close")
var ErrConflict = errors.New("transaction conflict")

const metaDelete int8 = 1 << 0

const itemType int8 = 1
const commitType int8 = 2

const maxKeyLen = 1024 * 1024        //1MB
const maxValueLen = 1024 * 1024 * 64 //64MB

type Item interface {
	Less(other btree.Item) bool
	MarshalBinary() (data []byte, err error)
	UnmarshalBinary(data []byte) error
}

type Transaction interface {
	Get(Item) Item
	ReplaceOrInsert(Item) Item
	Delete(Item)
	Commit() error
	CommitWith(cb func(err error))
	Discard() error
}
type DB interface {
	NewTransaction(write bool) (Transaction, error)
	Update(func(tx Transaction) error) error
	View(func(tx Transaction) error) error
	Close() error
}

type Options struct {
	BtreeDegree    int
	JournalDir     string
	SnapshotDir    string
	MaxJournalSize int64
	//Truncate journal files when reload journal error happen
	Truncate  bool
	SyncWrite bool
	New       func() Item
}

func OpenDB(options Options) (DB, error) {
	return openDB(options)
}

func openDB(options Options) (*db, error) {
	_assert(os.MkdirAll(options.JournalDir, 0777))
	_assert(os.MkdirAll(options.SnapshotDir, 0777))
	ctx, cancel := context.WithCancel(context.Background())
	var db = &db{
		Options:            options,
		ctx:                ctx,
		ctxCancel:          cancel,
		locker:             sync.RWMutex{},
		btree:              btree.New(options.BtreeDegree),
		btreeWithTSs:       nil,
		btreeWithTSsLock:   sync.RWMutex{},
		oracle:             newOracle(ctx),
		journal:            nil,
		journalC:           0,
		closeWG:            sync.WaitGroup{},
		isClose:            0,
		commitRequestQueue: block_queue.NewQueue(128),
	}
	if err := db.reload(); err != nil {
		cancel()
		return nil, err
	}
	if db.oracle.nextTS == 0 {
		db.oracle.nextTS = 1
	}
	doneUntil := db.oracle.nextTS - 1
	db.btreeWithTSs = append(db.btreeWithTSs, btreeWithTS{
		TS:    doneUntil,
		BTree: db.btree.Clone(),
	})
	db.oracle.readMark.doneUntil = doneUntil
	db.oracle.committedMark.doneUntil = doneUntil
	go db.writeCommitRequests()
	return db, nil
}

func DefaultOptions() Options {
	return Options{
		BtreeDegree:    10,
		JournalDir:     "journal",
		SnapshotDir:    "snapshot",
		MaxJournalSize: 1024 * 1024 * 128,
		Truncate:       false,
		SyncWrite:      true,
		New:            nil,
	}
}

func (opts Options) WithSyncWrite(val bool) Options {
	opts.SyncWrite = val
	return opts
}

func (opts Options) WithNew(f func() Item) Options {
	opts.New = f
	return opts
}
func (opts Options) WithBtreeDegree(val int) Options {
	opts.BtreeDegree = val
	return opts
}
func (opts Options) WithJournalDir(val string) Options {
	opts.JournalDir = val
	return opts
}
func (opts Options) WithSnapshotDir(val string) Options {
	opts.SnapshotDir = val
	return opts
}
func (opts Options) WithMaxJournalSize(val int64) Options {
	opts.MaxJournalSize = val
	return opts
}
func (opts Options) WithRecovery(val bool) Options {
	opts.Truncate = val
	return opts
}

type btreeWithTS struct {
	TS int64
	*btree.BTree
}

type db struct {
	Options
	ctx                context.Context
	ctxCancel          context.CancelFunc
	locker             sync.RWMutex
	btree              *btree.BTree
	btreeWithTSs       []btreeWithTS
	btreeWithTSsLock   sync.RWMutex
	oracle             *oracle
	journal            *journal
	journalC           int32
	closeWG            sync.WaitGroup
	isClose            int32
	commitRequestQueue *block_queue.Queue
}

type transaction struct {
	db          *db
	pending     *btree.BTree
	bTree       *btree.BTree
	isDiscard   bool
	readTs      int64
	readDone    bool
	committedTS int64
	sync        bool
}

type commitEntry struct {
	TS int64
}

type opRecord struct {
	item   Item
	delete bool
}

func (record *opRecord) UnmarshalBinary(data []byte) error {
	var reader = bytes.NewReader(data)
	if err := binary.Read(reader, binary.BigEndian, &record.delete); err != nil {
		return err
	}
	return record.item.UnmarshalBinary(data[1:])
}

func (record *opRecord) MarshalBinary() (data []byte, err error) {
	var buffer bytes.Buffer
	_ = binary.Write(&buffer, binary.BigEndian, record.delete)
	data, err = record.item.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buffer.Write(data)
	return buffer.Bytes(), nil
}

func (record *opRecord) Less(item btree.Item) bool {
	return record.item.Less(item.(*opRecord).item)
}

func (commitEntry *commitEntry) MarshalBinary() ([]byte, error) {
	var writer bytes.Buffer
	binary.Write(&writer, binary.BigEndian, commitEntry.TS)
	return writer.Bytes(), nil
}

func (commitEntry *commitEntry) UnmarshalBinary(data []byte) error {
	return binary.Read(bytes.NewReader(data), binary.BigEndian, &commitEntry.TS)
}

func (t *transaction) Get(key Item) Item {
	item := t.bTree.Get(key)
	if item == nil {
		return nil
	}
	return item.(Item)
}

var opRecordPool = sync.Pool{New: func() interface{} { return new(opRecord) }}
func (t *transaction) ReplaceOrInsert(item Item) Item {
	if t.pending == nil {
		panic("readOny transaction")
	}
	opRecord := opRecordPool.Get().(*opRecord)
	opRecord.item = item
	opRecord.delete = false
	t.pending.ReplaceOrInsert(opRecord)
	old := t.bTree.ReplaceOrInsert(item)
	if old == nil {
		return nil
	}
	return old.(Item)
}

func (t *transaction) Delete(item Item) {
	if t.pending == nil {
		panic("readOny transaction")
	}
	t.bTree.Delete(item)
	t.pending.ReplaceOrInsert(&opRecord{
		item:   item,
		delete: true,
	})
}

func NewEntryWithKey(key []byte) *KVItem {
	if key == nil {
		return nil
	}
	return &KVItem{key: key}
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (t *transaction) AscendRange(greaterOrEqual, lessThan Item, callback func(item Item) bool) {
	t.bTree.Clone().AscendRange(greaterOrEqual, lessThan, func(i btree.Item) bool {
		return callback(i.(Item))
	})
}

func (t *transaction) Commit() error {
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	defer t.Discard()
	if t.pending.Len() == 0 {
		return nil
	}
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
	defer t.Discard()
	if err := t.db.commit(t, func(err error) {
		go cb(err)
	}); err != nil {
		cb(err)
	}
}

func (t *transaction) Discard() error {
	if t.isDiscard {
		return nil
	}
	t.isDiscard = true
	t.db.oracle.ReadDone(t)
	return nil
}

func (db *db) checkClose() bool {
	return atomic.LoadInt32(&db.isClose) == 1
}

func (db *db) getReadTSBtree() (int64, *btree.BTree) {
	readTS := db.oracle.GetReadTS()
	db.btreeWithTSsLock.RLock()
	diff := readTS - db.btreeWithTSs[0].TS
	btree := db.btreeWithTSs[diff].BTree.Clone()
	db.btreeWithTSsLock.RUnlock()
	return readTS, btree
}

func (db *db) NewTransaction(update bool) (Transaction, error) {
	return db.newTransaction(update)
}

func (db *db) newTransaction(update bool) (*transaction, error) {
	if db.checkClose() {
		return nil, ErrDBClose
	}
	var pending *btree.BTree
	if update {
		pending = btree.New(db.BtreeDegree)
	}
	readTS, btree := db.getReadTSBtree()
	return &transaction{
		sync:    db.SyncWrite,
		db:      db,
		pending: pending,
		bTree:   btree,
		readTs:  readTS,
	}, nil
}

func (db *db) Update(update func(tx Transaction) error) error {
	tx, err := db.NewTransaction(true)
	if err != nil {
		return err
	}
	defer tx.Discard()
	if err := update(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *db) View(view func(tx Transaction) error) error {
	tx, err := db.NewTransaction(false)
	if err != nil {
		return err
	}
	defer tx.Discard()
	return view(tx)
}

func (db *db) Close() error {
	if atomic.CompareAndSwapInt32(&db.isClose, 0, 1) == false {
		return nil
	}
	//wait for committed update flush to log entry done
	db.oracle.readMark.DoneMark(db.oracle.GetReadTS())
	db.commitRequestQueue.Push(commitRequest{close: true})
	return nil
}

func (db *db) CloseWait() {
	_assert(db.Close())
	db.closeWG.Wait()
}

func (db *db) cleanupBtreeWithTSs() {
	readDoneUntil := db.oracle.readMark.DoneUntil()
	if len(db.btreeWithTSs) > 0 {
		diff := readDoneUntil - db.btreeWithTSs[0].TS
		_assertTrue(diff >= 0)
		if diff > 32 {
			copy(db.btreeWithTSs, db.btreeWithTSs[diff:])
			db.btreeWithTSs = db.btreeWithTSs[:len(db.btreeWithTSs)-int(diff)]
		}
	}
}

type commitRequest struct {
	journalBuf *bytes.Buffer
	pending    *btree.BTree
	sync       bool
	commitTS   int64
	close      bool
	callback   func(err error)
}

func (db *db) commit(tx *transaction, cb func(err error)) error {
	ts := tx.db.oracle.NewCommittedTs(tx)
	if ts == 0 {
		return errors.WithStack(ErrConflict)
	}
	db.oracle.ReadDone(tx)

	tx.committedTS = ts

	var buffer bytes.Buffer
	tx.pending.Descend(func(item btree.Item) bool {
		record := item.(*opRecord)
		data, err := record.MarshalBinary()
		_assert(err)
		_assert(binary.Write(&buffer, binary.BigEndian, itemType))
		_assert(binary.Write(&buffer, binary.BigEndian, int32(len(data))))
		_, err = buffer.Write(data)
		_assert(err)
		return true
	})

	entry := commitEntry{TS: ts}
	data, err := entry.MarshalBinary()
	_assert(err)
	_assert(binary.Write(&buffer, binary.BigEndian, commitType))
	_assert(binary.Write(&buffer, binary.BigEndian, int32(len(data))))
	_, err = buffer.Write(data)
	_assert(err)

	db.commitRequestQueue.Push(commitRequest{
		journalBuf: &buffer,
		pending:    tx.pending,
		sync:       tx.sync,
		commitTS:   tx.committedTS,
		callback:   cb,
	})
	return nil
}

func (db *db) doWriteCommitRequests(requests []commitRequest) {
	var sync bool
	journal := db.journal
	//write journal log
	for _, request := range requests {
		_assert(journal.append(request.commitTS, request.journalBuf))
		if request.sync {
			sync = true
		}
	}
	if sync {
		_assert(journal.sync())
	}
	//apply to btree
	for _, request := range requests {
		var cBTree = db.btree.Clone()
		request.pending.Descend(func(item btree.Item) bool {
			record := item.(*opRecord)
			if record.delete {
				cBTree.Delete(record.item)
			} else {
				cBTree.ReplaceOrInsert(record.item)
			}
			return true
		})
		db.btree = cBTree
		db.btreeWithTSsLock.Lock()
		db.cleanupBtreeWithTSs()
		db.btreeWithTSs = append(db.btreeWithTSs, btreeWithTS{
			TS:    request.commitTS,
			BTree: cBTree,
		})
		db.btreeWithTSsLock.Unlock()
	}
	//apply callback
	for _, request := range requests {
		db.oracle.CommittedDone(request.commitTS)
		request.callback(nil)
	}
	//compaction journal
	if db.journal.size() > db.MaxJournalSize &&
		atomic.CompareAndSwapInt32(&db.journalC, 0, 1) {
		db.journalCompaction(func() {
		})
	}

}

//for test
func (db *db) journalCompactionAndWait() {
	var wg sync.WaitGroup
	wg.Add(1)
	db.journalCompaction(func() {
		wg.Done()
	})
	wg.Wait()
}

func (db *db) journalCompaction(done func()) {
	db.closeWG.Add(2)
	defer db.closeWG.Done()
	var err error
	committedTS := db.journal.maxCommittedTS
	_assert(db.journal.close())
	_assert(db.journal.rename())
	name := filepath.Join(db.JournalDir,
		strconv.FormatInt(committedTS+1, 10)+journalExt)
	db.journal, err = openJournal(name, db.New)
	_assert(err)
	go func() {
		defer func() {
			atomic.StoreInt32(&db.journalC, 0)
			db.closeWG.Done()
			done()
		}()
		db.oracle.WaitCommittedTSMark(committedTS)
		tx, err := db.NewTransaction(false)
		if err == ErrDBClose {
			return
		}
		_assert(err)
		defer tx.Discard()
		t := tx.(*transaction)
		fmt.Println(t.readTs)
		_assert(db.makeSnapshot(t.bTree, snapshotHeader{CommittedTS: t.readTs}))
	}()
}

func (db *db) writeCommitRequests() {
	defer func() {
		_assert(db.journal.close())
		_assert(db.journal.rename())
		db.closeWG.Done()
	}()
	var entries int
	var isClose bool
	var commitRequests []commitRequest
	appendRequest := func(request commitRequest) {
		if request.close {
			isClose = true
			return
		}
		entries += request.pending.Len()
		commitRequests = append(commitRequests, request)
	}
	var items []interface{}
	for isClose == false {
		items = db.commitRequestQueue.PopAll(items)
		for _, item := range items {
			appendRequest(item.(commitRequest))
		}
		db.doWriteCommitRequests(commitRequests)
		commitRequests = commitRequests[:0]
		entries = 0
	}
}

const journalExt = ".log"
const snapshotExt = ".snap"

const snapshotExtTemp = ".snap.temp"

func (db *db) reload() error {
	if err := db.reloadSnapshot(); err != nil {
		return err
	}
	return db.reloadJournal(db.oracle.nextTS - 1)
}

type snapshotHeader struct {
	CommittedTS int64 `json:"committed_ts"`
}

func (h *snapshotHeader) decode(reader *bufio.Reader) error {
	var headerLen int32
	if err := binary.Read(reader, binary.BigEndian, &headerLen); err != nil {
		return errors.WithStack(err)
	}
	var buff = make([]byte, headerLen)
	if _, err := io.ReadFull(reader, buff); err != nil {
		return errors.WithStack(err)
	}
	return json.Unmarshal(buff, h)
}

func (h *snapshotHeader) encode(writer io.Writer) error {
	var data, _ = json.Marshal(h)
	if err := binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func (db *db) makeSnapshot(tree *btree.BTree, header snapshotHeader) error {
	tmp := filepath.Join(db.SnapshotDir, strconv.FormatInt(header.CommittedTS, 10)+snapshotExtTemp)
	f, err := os.Create(tmp)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(tmp)
	writer := bufio.NewWriter(f)
	_assert(header.encode(writer))
	var data []byte
	tree.Ascend(func(item btree.Item) bool {
		data, err = item.(Item).MarshalBinary()
		_assert(err)
		if err = binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
			err = errors.WithStack(err)
			return false
		}
		if _, err = writer.Write(data); err != nil {
			err = errors.WithStack(err)
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if err = f.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return os.Rename(tmp, strings.ReplaceAll(tmp, snapshotExtTemp, snapshotExt))
}

func (db *db) reloadSnapshot() error {
	var files []string
	err := filepath.Walk(db.SnapshotDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, snapshotExt) {
			files = append(files, path)
		} else if strings.HasSuffix(path, snapshotExtTemp) {
			_ = os.Remove(path) //delete last snapshot temp file
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if len(files) == 0 {
		return nil
	}
	sortFile(files)
	f, err := os.Open(files[len(files)-1])
	if err != nil {
		return errors.WithStack(err)
	}
	var reader = bufio.NewReader(f)
	var header snapshotHeader
	if err := header.decode(reader); err != nil {
		return errors.WithStack(err)
	}
	db.oracle.nextTS = header.CommittedTS + 1
	fmt.Println("reload snapshot committedTS", header.CommittedTS)
	for {
		var dataLen int32
		if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			return errors.WithStack(err)
		}
		var item = db.New()
		if err := item.UnmarshalBinary(data); err != nil {
			return errors.WithStack(err)
		}
		db.btree.ReplaceOrInsert(item)
	}
}

func (db *db) reloadJournal(committedTS int64) error {
	var files []string
	err := filepath.Walk(db.JournalDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, journalExt) {
			token := strings.SplitN(info.Name(), ".", 2)[0]
			ts, err := strconv.ParseInt(token, 10, 64)
			_assert(err)
			if ts <= committedTS {
				return nil
			}
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(files) == 0 {
		name := filepath.Join(db.JournalDir, "0"+journalExt)
		db.journal, err = openJournal(name, db.New)
		return err
	}
	sortFile(files)
	var records int64
	for _, file := range files {
		ts, err := strconv.ParseInt(strings.SplitN(filepath.Base(file), ".", 2)[0], 10, 64)
		_assert(err)
		if ts <= committedTS {
			continue
		}
		err = func() error {
			j, err := openJournal(file, db.New)
			if err != nil {
				return err
			}
			defer func() { _ = j.close() }()
			err = j.Range(func(ts int64, opRecords []opRecord) bool {
				records++
				if ts <= committedTS {
					return true
				}
				committedTS = ts
				db.oracle.nextTS = committedTS + 1
				for _, record := range opRecords {
					if record.delete {
						db.btree.Delete(record.item)
					} else {
						db.btree.ReplaceOrInsert(record.item)
					}
				}
				return true
			})
			if err != nil {
				if err == io.ErrUnexpectedEOF {
					if db.Truncate == false {
						return err
					}
					if err := j.truncate(); err != nil {
						return err
					}
					log.Println("journal reload error:unexpected EOF," +
						"truncate broken transaction,maybe lose data")
					return nil
				}
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	db.journal, err = openJournal(files[len(files)-1], db.New)
	_assert(err)
	db.journal.seekEnd()
	fmt.Println("reload journal committedTS", committedTS)
	fmt.Println("reload journal record count", records)
	return nil
}

type journal struct {
	sync.Mutex
	file           string
	f              *os.File
	maxCommittedTS int64
	reloadOffset   int64
	New            func() Item
}

func openJournal(filename string, New func() Item) (*journal, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &journal{
		file: filename,
		f:    f,
		New:  New,
	}, nil
}

func (j *journal) seekEnd() {
	_, err := j.f.Seek(0, io.SeekEnd)
	_assert(err)
}

func (j *journal) Range(callback func(committedTS int64, items []opRecord) bool) error {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var opRecords []opRecord
	reader := bufio.NewReader(j.f)
	for {
		var eType int8
		var dataLen int32
		if err := binary.Read(reader, binary.BigEndian, &eType); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
			return err
		}
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		if eType == itemType {
			var record = opRecord{item: j.New()}
			_assert(record.UnmarshalBinary(data))
			opRecords = append(opRecords, record)
		} else if eType == commitType {
			var commit commitEntry
			_assert(commit.UnmarshalBinary(data))
			callback(commit.TS, opRecords)
			opRecords = opRecords[:0]
			j.reloadOffset, _ = j.f.Seek(0, io.SeekCurrent)
		}
	}
}

func (j *journal) append(committedTS int64, reader io.Reader) error {
	if j.maxCommittedTS < committedTS {
		j.maxCommittedTS = committedTS
	}
	_, err := io.Copy(j.f, reader)
	return err
}

func (j *journal) sync() error {
	return j.f.Sync()
}

func (j *journal) rename() error {
	name := filepath.Join(filepath.Dir(j.file),
		strconv.FormatInt(j.maxCommittedTS, 10)+journalExt)
	return os.Rename(j.file, name)
}

func (j *journal) close() error {
	return j.f.Close()
}

func (j *journal) truncate() error {
	return j.f.Truncate(j.reloadOffset)
}

func (j *journal) size() int64 {
	info, err := j.f.Stat()
	_assert(err)
	return info.Size()
}

type committedTxn struct {
	ts           int64
	conflictKeys *btree.BTree
}

type oracle struct {
	locker        sync.Mutex
	committedMark *watermark
	readMark      *watermark
	doneTS        int64
	nextTS        int64
	committedTxns []committedTxn
}

func newOracle(ctx context.Context) *oracle {
	return &oracle{
		locker:        sync.Mutex{},
		committedMark: newWatermarkWithCtx(ctx),
		readMark:      newWatermarkWithCtx(ctx),
		doneTS:        0,
		nextTS:        0,
		committedTxns: nil,
	}
}

func (o *oracle) checkConflict(tx *transaction) bool {
	for _, it := range o.committedTxns {
		if it.ts < tx.readTs {
			continue
		}
		var conflict bool
		tx.pending.Ascend(func(i btree.Item) bool {
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

func (o *oracle) cleanCommittedTxn() {
	if len(o.committedTxns) == 0 {
		return
	}
	doneUntil := o.committedMark.DoneUntil()
	if doneUntil >= o.committedTxns[0].ts {
		diff := doneUntil - o.committedTxns[0].ts
		if diff > 8 {
			toGC := append([]committedTxn{},o.committedTxns[:diff]...)
			go func() {
				for _, txn := range toGC {
					txn.conflictKeys.Descend(func(i btree.Item) bool {
						record := i.(*opRecord)
						record.item = nil
						opRecordPool.Put(record)
						return true
					})
				}
			}()
			copy(o.committedTxns, o.committedTxns[diff:])
			o.committedTxns = o.committedTxns[:len(o.committedTxns)-int(diff)]
		}
	}
}

func (o *oracle) NewCommittedTs(tx *transaction) int64 {
	o.locker.Lock()
	defer o.locker.Unlock()
	if o.checkConflict(tx) {
		return 0
	}
	o.cleanCommittedTxn()
	ts := o.nextTS
	o.nextTS++
	o.committedMark.BeginMark(ts)
	o.committedTxns = append(o.committedTxns, committedTxn{
		ts:           ts,
		conflictKeys: tx.pending,
	})
	return ts
}

func (o *oracle) CommittedDone(ts int64) {
	o.committedMark.DoneMark(ts)
}

func (o *oracle) WaitCommittedTSMark(index int64) {
	//wait for committed index done
	_assert(o.committedMark.WaitForMark(context.Background(), index))
}

func (o *oracle) GetReadTS() int64 {
	o.locker.Lock()
	readTs := o.nextTS - 1
	o.locker.Unlock()
	o.readMark.BeginMark(readTs)

	//wait for committed index done
	_assert(o.committedMark.WaitForMark(context.Background(), readTs))
	return readTs
}

func (o *oracle) ReadDone(t *transaction) {
	if t.readDone == false {
		t.readDone = true
		o.readMark.DoneMark(t.readTs)
	}
}

type mark struct {
	index  int64
	done   bool
	waiter chan interface{}
}

// An Int64Heap is a min-heap of int64.
type Int64Heap []int64

func (h Int64Heap) Len() int {
	return len(h)
}

func (h Int64Heap) Less(i, j int) bool {
	return h[i] < h[j]
}
func (h Int64Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *Int64Heap) Push(x interface{}) {
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
	locker       sync.Mutex
	marks        chan mark
	doneUntil    int64
	int64Heap    *Int64Heap
	pendingCount map[int64]int
	waiters      map[int64][]chan interface{}
}

//for test
func newWatermark() *watermark {
	return newWatermarkWithCtx(context.Background())
}

func newWatermarkWithCtx(ctx context.Context) *watermark {
	wm := &watermark{
		marks:        make(chan mark),
		int64Heap:    new(Int64Heap),
		pendingCount: make(map[int64]int),
		waiters:      make(map[int64][]chan interface{}),
	}
	go wm.process(ctx)
	return wm
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
		done:  true,
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

func (wm *watermark) process(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-wm.marks:
			if m.waiter != nil {
				if wm.DoneUntil() >= m.index {
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

func sortFile(files []string) {
	sort.Slice(files, func(i, j int) bool {
		if len(files[i]) < len(files[j]) {
			return true
		}
		return files[i] < files[j]
	})
}

func _assertTrue(val bool) {
	if val == false {
		panic(fmt.Sprintf("%+v", errors.Errorf("assert failed")))
	}
}

func _assert(err error) {
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
}

type KVItem struct {
	key   []byte
	value []byte
}

func NewKVItem(key, value []byte) *KVItem {
	return &KVItem{
		key:   key,
		value: value,
	}
}

func (e *KVItem) Less(than btree.Item) bool {
	return bytes.Compare(e.key, than.(*KVItem).key) < 0
}

func (e *KVItem) MarshalBinary() ([]byte, error) {
	var buffer = new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, int32(len(e.key))); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, int32(len(e.value))); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(e.key); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(e.value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (e *KVItem) UnmarshalBinary(data []byte) error {
	var keyLen int32
	var valLen int32
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
		return err
	}
	e.key = make([]byte, keyLen)
	e.value = make([]byte, valLen)
	if _, err := io.ReadFull(reader, e.key); err != nil {
		return err
	}
	if _, err := io.ReadFull(reader, e.value); err != nil {
		return err
	}
	return nil
}
