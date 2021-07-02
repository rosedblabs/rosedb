package hash

// the implementation of hash table.

type (
	// Hash hash table struct.
	Hash struct {
		record Record
	}

	// Record hash records to save.
	Record map[string]map[string][]byte
)

// New create a new hash ds.
func New() *Hash {
	return &Hash{make(Record)}
}

// HSet Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
func (h *Hash) HSet(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if h.record[key][field] != nil {
		// if this field exists, overwritten it.
		h.record[key][field] = value
	} else {
		// create if this field not exists.
		h.record[key][field] = value
		res = 1
	}
	return
}

// HSetNx sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
// Return if the operation successful
func (h *Hash) HSetNx(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
		return 1
	}
	return 0
}

// HGet returns the value associated with field in the hash stored at key.
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

// HGetAll returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
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
func (h *Hash) HExists(key, field string) (res int) {
	if !h.exist(key) {
		return
	}

	if _, exist := h.record[key][field]; exist {
		res = 1
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
func (h *Hash) HVals(key string) (val [][]byte) {
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
