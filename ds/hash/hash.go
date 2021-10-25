package hash

import "github.com/roseduan/rosedb/index"

// the implementation of hash table.

type (
	// Hash hash table struct.
	Hash struct {
		record Record
	}

	// Record hash records to save.
	Record map[string]map[string]*index.Indexer
)

// New create a new hash ds.
func New() *Hash {
	return &Hash{make(Record)}
}

// HSet put indexer in memory.
func (h *Hash) HSet(idx *index.Indexer) (res int) {
	key := string(idx.Meta.Key)
	field := string(idx.Meta.Extra)
	if !h.exist(key) {
		h.record[key] = make(map[string]*index.Indexer)
	}

	if _, exist := h.record[key][field]; !exist {
		res = 1
	}

	h.record[key][field] = idx
	return
}

// HGet returns the value associated with field in the hash stored at key.
func (h *Hash) HGet(key, field string) *index.Indexer {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

// HGetAll returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
func (h *Hash) HGetAll(key string) (res []*index.Indexer) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		res = append(res, v)
	}
	return
}

// HDel removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (h *Hash) HDel(key, field string) int {
	if !h.exist(key) {
		return 0
	}

	if _, exist := h.record[key][field]; exist {
		delete(h.record[key], field)
		return 1
	}
	return 0
}

// HKeyExists returns if key exists in hash.
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

// HExists returns if field is an existing field in the hash stored at key.
func (h *Hash) HExists(key, field string) (ok bool) {
	if !h.exist(key) {
		return
	}

	if _, exist := h.record[key][field]; exist {
		ok = true
	}
	return
}

// HLen returns the number of fields contained in the hash stored at key.
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

// HKeys returns all field names in the hash stored at key.
func (h *Hash) HKeys(key string) (val []string) {
	if !h.exist(key) {
		return
	}

	for k := range h.record[key] {
		val = append(val, k)
	}
	return
}

// HVals returns all values in the hash stored at key.
func (h *Hash) HVals(key string) (val []*index.Indexer) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return
}

// HClear clear the key in hash.
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
