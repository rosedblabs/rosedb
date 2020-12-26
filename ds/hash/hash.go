package hash

type (
	Hash struct {
		record Record
	}

	Record map[string]map[string][]byte
)

func New() *Hash {
	return &Hash{make(Record)}
}

//将哈希表 hash 中域 field 的值设置为 value
//如果给定的哈希表并不存在， 那么一个新的哈希表将被创建并执行 HSet 操作
//如果域 field 已经存在于哈希表中， 那么它的旧值将被新值 value 覆盖
func (h *Hash) HSet(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	h.record[key][field] = value
	return len(h.record[key])
}

//返回哈希表中给定域的值
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}

	return h.record[key][field]
}

//返回哈希表 key 中，所有的域和值
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}

	return
}

//删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略
//返回被成功移除的元素个数
func (h *Hash) HDel(key string, fields ...string) (res int) {
	if !h.exist(key) {
		return 0
	}

	for _, field := range fields {
		if _, exist := h.record[key][field]; exist {
			delete(h.record[key], field)
			res++
		}
	}

	return
}

//检查给定域 field 是否存在于key对应的哈希表中
func (h *Hash) HExists(key, field string) bool {
	if !h.exist(key) {
		return false
	}

	_, exist := h.record[key][field]
	return exist
}

//返回哈希表 key 中域的数量
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}

	return len(h.record[key])
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
