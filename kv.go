

type KV struct {
	Path string
	fd int
	tree BTree
}

type FreeList struct {
// persisted data in the meta page
headPage uint64 // pointer to the list head node
headSeq  uint64 // monotonic sequence number to index into the list head
tailPage uint64
tailSeq  uint64
// in-memory states
maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
}

func (db *KV) Open() error

func (db *KV) Get(key []byte) ([]byte, bool) {
return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
db.tree.Insert(key, val)
return updateFile(db)
}
func (db *KV) Del(key []byte) (bool, error) {
deleted := db.tree.Delete(key)
return deleted, updateFile(db)
}