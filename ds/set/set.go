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

func (s *Set) SAdd(key string, member []byte) int {
	if !s.exist(key) {
		s.record[key] = make(map[string]bool)
	}

	s.record[key][string(member)] = true

	return len(s.record[key])
}

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

func (s *Set) SIsMember(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}

	return s.record[key][string(member)]
}

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

func (s *Set) SRem(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}

	if ok := s.record[key][string(member)]; ok {
		delete(s.record[key], string(member))
		return true
	}

	return false
}

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

func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}

	return len(s.record[key])
}

func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}

	for k := range s.record[key] {
		val = append(val, []byte(k))
	}

	return
}

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
