package index

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
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
		wrapper := &ChunkPositionWrapper{pos: chunkPosition, deleted: false}
		_, err := vi.putVector(vector, wrapper)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet := vi.getVectorTest(vectorArr[3], 3)
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
		vi.Put(key, chunkPosition)
	}

	resSet := vi.getVectorTest(govector.Vector{8, 7}, 3)
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestVectorIndex_Simple_Delete(t *testing.T) {
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
		vi.Put(key, chunkPosition)
	}

	// delete
	vi.Delete(EncodeVector(govector.Vector{8, 10}))

	resSet := vi.getVectorTest(govector.Vector{8, 7}, 3)

	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestThroughput_test(t *testing.T) {
	VectorSize := uint32(10)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(30)
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
	var i uint32
	for i = 0; i < originalFileItem; i++ {
		key := EncodeVector(vecArr[i])
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vecArr[i], chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	var wg sync.WaitGroup
	now = time.Now()
	for i = 0; i < testFileItem; i++ {
		wg.Add(1)
		go func(key govector.Vector) {
			defer wg.Done()
			resultArr, err := vi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("vector_index", originalFileItem, testFileItem, putTime, getTime)
}

func TestThroughput_test_10(t *testing.T) {
	VectorSize := uint32(10)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(30)
	originalFileItem := uint32(10000)
	testFileItem := uint32(10000)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_10.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_10.txt", VectorSize)

	now := time.Now()
	// put vector into db
	var i uint32
	for i = 0; i < originalFileItem; i++ {
		key := EncodeVector(vecArr[i])
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vecArr[i], chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	var wg sync.WaitGroup
	now = time.Now()
	for i = 0; i < testFileItem; i++ {
		wg.Add(1)
		go func(key govector.Vector) {
			defer wg.Done()
			resultArr, err := vi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("vector_index", originalFileItem, testFileItem, putTime, getTime)
}

func TestThroughput_test_50(t *testing.T) {
	VectorSize := uint32(50)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(30)
	originalFileItem := uint32(10000)
	testFileItem := uint32(10000)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_50.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_50.txt", VectorSize)

	now := time.Now()
	// put vector into db
	var i uint32
	for i = 0; i < originalFileItem; i++ {
		key := EncodeVector(vecArr[i])
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vecArr[i], chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	var wg sync.WaitGroup
	now = time.Now()
	for i = 0; i < testFileItem; i++ {
		wg.Add(1)
		go func(key govector.Vector) {
			defer wg.Done()
			resultArr, err := vi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("vector_index", originalFileItem, testFileItem, putTime, getTime)
}

func TestThroughput_test_100(t *testing.T) {
	VectorSize := uint32(100)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(30)
	originalFileItem := uint32(10000)
	testFileItem := uint32(10000)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_100.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_100.txt", VectorSize)

	now := time.Now()
	// put vector into db
	var i uint32
	for i = 0; i < originalFileItem; i++ {
		key := EncodeVector(vecArr[i])
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vecArr[i], chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	var wg sync.WaitGroup
	now = time.Now()
	for i = 0; i < testFileItem; i++ {
		wg.Add(1)
		go func(key govector.Vector) {
			defer wg.Done()
			resultArr, err := vi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("vector_index", originalFileItem, testFileItem, putTime, getTime)
}

func TestThroughput_test_500(t *testing.T) {
	VectorSize := uint32(500)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(30)
	originalFileItem := uint32(10000)
	testFileItem := uint32(10000)

	// initiate database
	vi := newVectorIndex(m, maxM, interval)
	w, _ := wal.Open(wal.DefaultOptions)

	// load data from txt file
	vecArr := loadVectorFromTxt("../test_files/vectors_500.txt", VectorSize)
	testArr := loadVectorFromTxt("../test_files/testData/vectors_500.txt", VectorSize)

	now := time.Now()
	// put vector into db
	var i uint32
	for i = 0; i < originalFileItem; i++ {
		key := EncodeVector(vecArr[i])
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vecArr[i], chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	var wg sync.WaitGroup
	now = time.Now()
	for i = 0; i < testFileItem; i++ {
		wg.Add(1)
		go func(key govector.Vector) {
			defer wg.Done()
			resultArr, err := vi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("vector_index", originalFileItem, testFileItem, putTime, getTime)
}

func TestThroughput_test_1000(t *testing.T) {
	VectorSize := uint32(1000)
	m := uint32(3)
	maxM := uint32(5)
	interval := uint32(5)
	resultSize := uint32(30)
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
	var i uint32
	for i = 0; i < originalFileItem; i++ {
		key := EncodeVector(vecArr[i])
		chunkPosition, _ := w.Write(key)
		_, err := vi.PutVector(vecArr[i], chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}
	putTime := time.Since(now)

	var wg sync.WaitGroup
	now = time.Now()
	for i = 0; i < testFileItem; i++ {
		wg.Add(1)
		go func(key govector.Vector) {
			defer wg.Done()
			resultArr, err := vi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("vector_index", originalFileItem, testFileItem, putTime, getTime)
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
	var vecArr []govector.Vector
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

func printReport(filename string, originalFileItem uint32, testFileItem uint32, putTime time.Duration, getTime time.Duration) {

	fmt.Println("\n---------------------------------Here is the report ----------------------------")
	fmt.Println("time to put all", originalFileItem, "items is ", putTime.Seconds(), "s")
	get_throughput := float64(originalFileItem) / putTime.Seconds()
	fmt.Println("throughput is ", get_throughput, "qps")
	fmt.Println("time to get result for all", testFileItem, "items is ", getTime.Seconds(), "s")
	put_throughput := float64(testFileItem) / getTime.Seconds()
	fmt.Println("throughput is ", put_throughput, "qps")

	resultsFolder := "../test_files/resultsData/"
	resultsFilePath := resultsFolder + filename
	file, err := os.Create(resultsFilePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprintf("%v %v %v %v", putTime.Seconds(), get_throughput, getTime.Seconds(), put_throughput))
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
}
