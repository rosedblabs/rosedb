package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/drewlanenga/govector"
)

func BytesToVector(b []byte) (govector.Vector, error) {

	s := string(b)
	elements := strings.Split(strings.Trim(s, "()"), ",")
	vec := make([]float64, len(elements))

	for i, element := range elements {
		val, err := strconv.ParseFloat(element, 64)
		if err != nil {
			fmt.Println("input is not a valid vector")
			return nil, err
		}
		vec[i] = val
	}
	govec, err := govector.AsVector(vec)
	// print each element of the vector
	for i := 0; i < len(govec); i++ {
		fmt.Println(govec[i])
	}
	return govec, err
}

func main() {
	BytesToVector([]byte("(1,2,3,4.5,5)"))
}
