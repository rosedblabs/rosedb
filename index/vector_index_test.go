package index

import (
	"bufio"
	"fmt"
	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVectorIndex_Put_Get(t *testing.T) {
	vi := newVectorIndex(3, 5, 5)
	w, _ := wal.Open(wal.DefaultOptions)

	var vectorArr = []govector.Vector{{8, -7, -10, -8, 3, -6, 6, -2, 5, 1},
		{-2, -2, -6, -10, 10, -3, 1, 3, -9, -10},
		{-4, 7, -6, -1, 3, -5, 5, -2, -10, -3},
		{1, 0, -7, 1, 3, -3, 1, 0, -2, 7},
		{-3, -7, -6, -3, 5, 3, 1, 1, -6, 6},
		{9, 0, 8, -3, -4, 1, -3, -9, -10, 4},
		{8, -5, -7, 4, -10, 0, -7, 4, 10, 0},
		{-2, -10, -7, -1, -10, -4, 1, 2, -3, 3},
		{-1, -7, 6, 2, -2, -2, -2, -1, -2, -10},
		{9, -2, -1, -1, -6, 9, 2, 3, -7, 5},
	}

	for _, vector := range vectorArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet, err := vi.GetVector(vectorArr[3], 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestVectorIndex_Simple_Put_Get(t *testing.T) {
	vi := newVectorIndex(3, 5, 5)
	w, _ := wal.Open(wal.DefaultOptions)

	var vectorArr = []govector.Vector{{1, 2},
		{4, 8},
		{4, 9},
		{8, 10},
		{10, 12},
		{10, 6},
		{15, 3},
		{5, 4},
		{6, 7},
		{8, 3},
		{2, 9},
		{12, 5},
		{14, 2},
	}

	for _, vector := range vectorArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet, err := vi.GetVector(govector.Vector{8, 7}, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestThroughput_test(t *testing.T) {
	VectorSize := uint32(10)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(3)
	originalFileItem := uint32(10)
	testFileItem := uint32(10)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_test.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_test.txt", VectorSize)

	now := time.Now()
	// put vector into db
	for _, vector := range vecArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	now = time.Now()
	for _, vector := range testArr {
		resultArr, err := vi.GetVector(vector, resultSize)
		if err != nil {
			t.Fatalf("get failed: %v", err.Error())
		}
		fmt.Println(resultArr)
	}
	getTime := time.Since(now)
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")
}

func TestThroughput_test_10(t *testing.T) {
	VectorSize := uint32(10)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(3)
	originalFileItem := uint32(10000)
	testFileItem := uint32(100)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_10.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_10.txt", VectorSize)

	now := time.Now()
	// put vector into db
	for _, vector := range vecArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	now = time.Now()
	for _, vector := range testArr {
		resultArr, err := vi.GetVector(vector, resultSize)
		if err != nil {
			t.Fatalf("get failed: %v", err.Error())
		}
		fmt.Println(resultArr)
	}
	getTime := time.Since(now)
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")
}

func TestThroughput_test_50(t *testing.T) {
	VectorSize := uint32(50)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(3)
	originalFileItem := uint32(10000)
	testFileItem := uint32(100)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_50.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_50.txt", VectorSize)

	now := time.Now()
	// put vector into db
	for _, vector := range vecArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	now = time.Now()
	for _, vector := range testArr {
		resultArr, err := vi.GetVector(vector, resultSize)
		if err != nil {
			t.Fatalf("get failed: %v", err.Error())
		}
		fmt.Println(resultArr)
	}
	getTime := time.Since(now)
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")
}

func TestThroughput_test_100(t *testing.T) {
	VectorSize := uint32(100)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(3)
	originalFileItem := uint32(10000)
	testFileItem := uint32(100)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_100.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_100.txt", VectorSize)

	now := time.Now()
	// put vector into db
	for _, vector := range vecArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	now = time.Now()
	for _, vector := range testArr {
		resultArr, err := vi.GetVector(vector, resultSize)
		if err != nil {
			t.Fatalf("get failed: %v", err.Error())
		}
		fmt.Println(resultArr)
	}
	getTime := time.Since(now)
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")
}

func TestThroughput_test_500(t *testing.T) {
	VectorSize := uint32(500)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(3)
	originalFileItem := uint32(10000)
	testFileItem := uint32(100)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_500.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_500.txt", VectorSize)

	now := time.Now()
	// put vector into db
	for _, vector := range vecArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	now = time.Now()
	for _, vector := range testArr {
		resultArr, err := vi.GetVector(vector, resultSize)
		if err != nil {
			t.Fatalf("get failed: %v", err.Error())
		}
		fmt.Println(resultArr)
	}
	getTime := time.Since(now)
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")

}

func TestThroughput_test_1000(t *testing.T) {
	VectorSize := uint32(1000)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(3)
	originalFileItem := uint32(10000)
	testFileItem := uint32(100)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_1000.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_1000.txt", VectorSize)

	now := time.Now()
	// put vector into db
	for _, vector := range vecArr {
		key := EncodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	now = time.Now()
	for _, vector := range testArr {
		resultArr, err := vi.GetVector(vector, resultSize)
		if err != nil {
			t.Fatalf("get failed: %v", err.Error())
		}
		fmt.Println(resultArr)
	}
	getTime := time.Since(now)
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")

}
func loadVectorFromTxt(fileName string, VectorSize uint32) []govector.Vector {
	// read vector from file
	fmt.Println("loading vectors from txt file ......")
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file:", err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	vecArr := []govector.Vector{}
	for scanner.Scan() {
		line := scanner.Text()
		vec := make(govector.Vector, VectorSize)
		numbers := strings.Split(line, " ")
		for idx, num := range numbers {
			floatNum, err := strconv.ParseFloat(num, 64)
			if err != nil {
				fmt.Println(err)
				continue
			}
			vec[idx] = floatNum
		}
		//encodeVec := EncodeVector(vec)
		vecArr = append(vecArr, vec)
	}
	fmt.Println("load vectors success")
	return vecArr
}
