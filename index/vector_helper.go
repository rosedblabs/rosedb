package index

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/drewlanenga/govector"
)


func EncodeVector(v govector.Vector) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(v)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return buffer.Bytes()
}

func DecodeVector(data []byte) govector.Vector {
	var vector govector.Vector
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&vector)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return vector
}