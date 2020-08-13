package mmdb

const (
	KeyTypeInt64 KeyType = 1
)

type KeyType int

type Key interface {
	Type() KeyType
	Value() interface{}
}

func newKey(obj interface{}) Key {
	switch val := obj.(type) {
	case int64:
		return &int64Key{val: val}
	}
	return nil
}

type int64Key struct {
	val int64
}

func (KeyType KeyType) String() string {
	switch KeyType {
	case KeyTypeInt64:
		return "int64"
	default:
		panic("unknown type")
	}
}

func (i int64Key) Type() KeyType {
	return KeyTypeInt64
}

func (i int64Key) Value() interface{} {
	return i.val
}
