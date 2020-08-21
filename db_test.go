package mmdb

import (
	"fmt"
	"github.com/google/btree"
	"testing"
)

func TestName(t *testing.T) {
	tree := btree.New(3)

	count := 0
	for ; count < 100; count++ {
		tree.ReplaceOrInsert(btree.Int(count))
	}

	tree.Descend(func(i btree.Item) bool {
		fmt.Println(i)
		return true
	})
}
