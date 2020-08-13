package mmdb

type DB interface {
	CreateTable(name string, primaryKey string, table interface{}) error
	CreateIndex(table string, uniq bool, indexKey string) error
	ListIndex(table string) ([]string, error)
	Query(table string, key string, value interface{}) ([]interface{}, error)
	Insert(table string, obj interface{}) error
	Update(table string, obj interface{}) error
}

//mmdb memory database
type mmdb struct {
}

func (m *mmdb) CreateTable(name string, primaryKey string, table interface{}) error {
	panic("implement me")
}

func (m *mmdb) CreateIndex(table string, uniq bool, indexKey string) error {
	panic("implement me")
}

func (m *mmdb) ListIndex(table string) ([]string, error) {
	panic("implement me")
}

func (m *mmdb) Query(table string, key string, value interface{}) ([]interface{}, error) {
	panic("implement me")
}

func (m *mmdb) Insert(table string, obj interface{}) error {
	panic("implement me")
}

func (m *mmdb) Update(table string, obj interface{}) error {
	panic("implement me")
}

func NewMMDB() *mmdb {
	return &mmdb{}
}
