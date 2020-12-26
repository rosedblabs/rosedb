package set

type (
	Set struct {
		record Record
	}

	Record map[string]map[string]bool
)

func New() *Set {
	return &Set{make(Record)}
}

//添加元素，返回添加后的集合中的元素个数
func (s *Set) SAdd(key string, members ...[]byte) int {
	if !s.exist(key) {
		s.record[key] = make(map[string]bool)
	}

	for _, val := range members {
		s.record[key][string(val)] = true
	}

	return len(s.record[key])
}

//随机移除并返回集合中的count个元素
func (s *Set) SPop(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count <= 0 {
		return val
	}

	for k, _ := range s.record[key] {
		delete(s.record[key], k)
		val = append(val, []byte(k))

		count--
		if count == 0 {
			break
		}
	}

	return val
}

//判断 member 元素是不是集合 key 的成员
func (s *Set) SIsMember(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}

	return s.record[key][string(member)]
}

//从集合中返回随机元素，count的可选值如下：
//如果 count 为正数，且小于集合元素数量，则返回一个包含 count 个元素的数组，数组中的元素各不相同
//如果 count 大于等于集合元素数量，那么返回整个集合
//如果 count 为负数，则返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值
func (s *Set) SRandMember(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count == 0 {
		return val
	}

	if count > 0 {
		for k := range s.record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.record[key] {
				return []byte(k)
			}
			return nil
		}

		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}

	return val
}

//移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略
//被成功移除的元素的数量，不包括被忽略的元素
func (s *Set) SRem(key string, members ...[]byte) (res int) {
	if !s.exist(key) {
		return 0
	}

	for _, val := range members {
		if ok := s.record[key][string(val)]; ok {
			delete(s.record[key], string(val))
			res++
		}
	}

	return
}

//将 member 元素从 src 集合移动到 dst 集合
func (s *Set) SMove(src, dst string, member []byte) bool {
	if !s.exist(src) {
		return false
	}

	if !s.exist(dst) {
		s.record[dst] = make(map[string]bool)
	}

	delete(s.record[src], string(member))
	s.record[dst][string(member)] = true

	return true
}

//返回集合中的元素个数
func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}

	return len(s.record[key])
}

//返回集合中的所有元素
func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}

	for k := range s.record[key] {
		val = append(val, []byte(k))
	}

	return
}

//返回给定全部集合数据的并集
func (s *Set) SUnion(keys ...string) (val [][]byte) {

	m := make(map[string]bool)
	for _, k := range keys {
		if s.exist(k) {
			for v := range s.record[k] {
				m[v] = true
			}
		}
	}

	for v := range m {
		val = append(val, []byte(v))
	}

	return
}

//返回给定集合数据的差集
func (s *Set) SDiff(keys ...string) (val [][]byte) {

	if len(keys) < 2 || !s.exist(keys[0]) {
		return
	}

	for v := range s.record[keys[0]] {

		flag := true
		for i := 1; i < len(keys); i++ {
			if s.SIsMember(keys[i], []byte(v)) {
				flag = false
				break
			}
		}

		if flag {
			val = append(val, []byte(v))
		}
	}

	return
}

//key对应的集合是否存在
func (s *Set) exist(key string) bool {
	_, exist := s.record[key]
	return exist
}
