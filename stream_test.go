/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

func keyWithPrefix(prefix string, k int) []byte {
	return []byte(fmt.Sprintf("%s-%d", prefix, k))
}

func keyToInt(k []byte) (string, int) {
	splits := strings.Split(string(k), "-")
	key, err := strconv.Atoi(splits[1])
	y.Check(err)
	return splits[0], key
}

func value(k int) []byte {
	return []byte(fmt.Sprintf("%08d", k))
}

type collector struct {
	kv []*bpb.KV
}

func (c *collector) Send(buf *z.Buffer) error {
	list, err := BufferToKVList(buf)
	if err != nil {
		return err
	}
	for _, kv := range list.Kv {
		if kv.StreamDone == true {
			return nil
		}
		cp := proto.Clone(kv).(*bpb.KV)
		c.kv = append(c.kv, cp)
	}
	return err
}

var ctxb = context.Background()

func TestStream(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= 100; i++ {
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = c.Send

	// Test case 1. Retrieve everything.
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 300, len(c.kv), "Expected 300. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, 100, count, "Count mismatch for pred: %s", pred)
	}

	// Test case 2. Retrieve only 1 predicate.
	stream.Prefix = []byte("p1")
	c.kv = c.kv[:0]
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 100, len(c.kv), "Expected 100. Got: %d", len(c.kv))

	m = make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 1, len(m))
	for pred, count := range m {
		require.Equal(t, 100, count, "Count mismatch for pred: %s", pred)
	}

	// Test case 3. Retrieve select keys within the predicate.
	c.kv = c.kv[:0]
	stream.ChooseKey = func(item *Item) bool {
		_, k := keyToInt(item.Key())
		return k%2 == 0
	}
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 50, len(c.kv), "Expected 50. Got: %d", len(c.kv))

	m = make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 1, len(m))
	for pred, count := range m {
		require.Equal(t, 50, count, "Count mismatch for pred: %s", pred)
	}

	// Test case 4. Retrieve select keys from all predicates.
	c.kv = c.kv[:0]
	stream.Prefix = []byte{}
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 150, len(c.kv), "Expected 150. Got: %d", len(c.kv))

	m = make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, 50, count, "Count mismatch for pred: %s", pred)
	}
	require.NoError(t, db.Close())
}

func TestStreamMaxSize(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	// Set the maxStreamSize to 1MB for the duration of the test so that the it can use a smaller
	// dataset than it would otherwise need.
	originalMaxStreamSize := maxStreamSize
	maxStreamSize = 1 << 20
	defer func() {
		maxStreamSize = originalMaxStreamSize
	}()

	testSize := int(1e6)
	dir, err := os.MkdirTemp("", "badger-big-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	wb := db.NewWriteBatchAt(5)
	for _, prefix := range []string{"p0", "p1", "p2"} {
		for i := 1; i <= testSize; i++ {
			require.NoError(t, wb.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
	}
	require.NoError(t, wb.Flush())

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = c.Send

	// default value
	require.Equal(t, stream.MaxSize, maxStreamSize)

	// reset maxsize
	stream.MaxSize = 1024 * 1024 * 50

	// Test case 1. Retrieve everything.
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 3*testSize, len(c.kv), "Expected 30000. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, testSize, count, "Count mismatch for pred: %s", pred)
	}
	require.NoError(t, db.Close())
}

func TestStreamWithThreadId(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= 100; i++ {
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	stream.KeyToList = func(key []byte, itr *Iterator) (
		*bpb.KVList, error) {
		require.Less(t, itr.ThreadId, stream.NumGo)
		return stream.ToList(key, itr)
	}
	c := &collector{}
	stream.Send = c.Send

	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 300, len(c.kv), "Expected 300. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, 100, count, "Count mismatch for pred: %s", pred)
	}
	require.NoError(t, db.Close())
}

func TestBigStream(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	// Set the maxStreamSize to 1MB for the duration of the test so that the it can use a smaller
	// dataset than it would otherwise need.
	originalMaxStreamSize := maxStreamSize
	maxStreamSize = 1 << 20
	defer func() {
		maxStreamSize = originalMaxStreamSize
	}()

	testSize := int(1e6)
	dir, err := os.MkdirTemp("", "badger-big-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	wb := db.NewWriteBatchAt(5)
	for _, prefix := range []string{"p0", "p1", "p2"} {
		for i := 1; i <= testSize; i++ {
			require.NoError(t, wb.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
	}
	require.NoError(t, wb.Flush())

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = c.Send

	// Test case 1. Retrieve everything.
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 3*testSize, len(c.kv), "Expected 30000. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, testSize, count, "Count mismatch for pred: %s", pred)
	}
	require.NoError(t, db.Close())
}

// There was a bug in the stream writer code which would cause allocators to be
// freed up twice if the default keyToList was not used. This test verifies that issue.
func TestStreamCustomKeyToList(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, key := range []string{"p0", "p1", "p2"} {
		for i := 1; i <= 100; i++ {
			txn := db.NewTransactionAt(math.MaxUint64, true)
			require.NoError(t, txn.SetEntry(NewEntry([]byte(key), value(i))))
			count++
			require.NoError(t, txn.CommitAt(uint64(i), nil))
		}
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	stream.KeyToList = func(key []byte, itr *Iterator) (*bpb.KVList, error) {
		item := itr.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kv := &bpb.KV{
			Key:   y.Copy(item.Key()),
			Value: val,
		}
		return &bpb.KVList{
			Kv: []*bpb.KV{kv},
		}, nil
	}
	res := map[string]struct{}{"p0": {}, "p1": {}, "p2": {}}
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		require.NoError(t, err)
		for _, kv := range list.Kv {
			key := string(kv.Key)
			if _, ok := res[key]; !ok {
				panic(fmt.Sprintf("%s key not found", key))
			}
			delete(res, key)
		}
		return nil
	}
	require.NoError(t, stream.Orchestrate(ctxb))
	require.Zero(t, len(res))
}

// A stream created from a read-only transaction reads its snapshot in every goroutine: the keys
// set, changed or deleted after the transaction began are read as they were then.
func TestStreamReadsTheSnapshotOfItsTxn(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		prefixes := []string{"p0", "p1", "p2", "p3"}
		for _, prefix := range prefixes {
			require.NoError(t, db.Update(func(txn *Txn) error {
				for i := 1; i <= 100; i++ {
					if err := txn.Set(keyWithPrefix(prefix, i), value(i)); err != nil {
						return err
					}
				}
				return nil
			}))
		}
		snapshot := db.NewTransaction(false)
		defer snapshot.Discard()
		require.NoError(t, db.Update(func(txn *Txn) error {
			for _, prefix := range prefixes {
				if err := txn.Set(keyWithPrefix(prefix, 1), []byte("changed")); err != nil {
					return err
				}
				if err := txn.Set(keyWithPrefix(prefix, 101), value(101)); err != nil {
					return err
				}
				if err := txn.Delete(keyWithPrefix(prefix, 2)); err != nil {
					return err
				}
			}
			return nil
		}))

		stream := snapshot.NewStream()
		c := &collector{}
		stream.Send = c.Send
		require.NoError(t, stream.Orchestrate(ctxb))
		require.Len(t, c.kv, 400)
		for _, kv := range c.kv {
			_, i := keyToInt(kv.Key)
			require.Equal(t, value(i), kv.Value, "%s", kv.Key)
			require.LessOrEqual(t, kv.Version, snapshot.ReadTs())
		}
	})
}

// A stream does not read a transaction discarded: the versions it read may be gone.
func TestStreamRefusesADiscardedTxn(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		snapshot := db.NewTransaction(false)
		stream := snapshot.NewStream()
		snapshot.Discard()
		stream.Send = (&collector{}).Send
		require.ErrorIs(t, stream.Orchestrate(ctxb), ErrDiscardedTxn)
	})
}

// The goroutines of a stream created from the DB read at one read timestamp, that of the snapshot
// taken when Orchestrate starts, while commits go on: a commit landing between the starts of two
// of them is read by none.
func TestStreamReadsOneSnapshotWhileCommitsGoOn(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 1; i <= 2000; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set(keyWithPrefix(fmt.Sprintf("p%d", i%16), i), value(i))
			}))
		}
		stop := make(chan struct{})
		written := make(chan error, 1)
		go func() {
			for i := 1; ; i++ {
				select {
				case <-stop:
					written <- nil
					return
				default:
				}
				if err := db.Update(func(txn *Txn) error {
					return txn.Set(keyWithPrefix("later", i), value(i))
				}); err != nil {
					written <- err
					return
				}
			}
		}()
		for range 20 {
			var mu sync.Mutex
			readTs := map[uint64]bool{}
			stream := db.NewStream()
			stream.NumGo = 16
			stream.KeyToList = func(key []byte, itr *Iterator) (*bpb.KVList, error) {
				mu.Lock()
				readTs[itr.readTs] = true
				mu.Unlock()
				return stream.ToList(key, itr)
			}
			stream.Send = (&collector{}).Send
			require.NoError(t, stream.Orchestrate(ctxb))
			require.Len(t, readTs, 1, "the goroutines read at one timestamp")
		}
		close(stop)
		require.NoError(t, <-written)
	})
}
