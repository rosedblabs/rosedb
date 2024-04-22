package index

import (
	"bytes"
	"encoding/gob"
	"fmt"
)


func EncodeVector(v RoseVector) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(v)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return buffer.Bytes()
}

func DecodeVector(data []byte) RoseVector {
	var vector RoseVector
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&vector)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return vector
}

type RoseVector []float32