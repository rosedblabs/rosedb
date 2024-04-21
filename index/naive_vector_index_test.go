package index

import (
	"fmt"
	"testing"

	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
	"sync"
	"time"
)

func TestNaiveVector_Put_Get(t *testing.T) {
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet, err := nvi.GetVector(govector.Vector{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestNaiveVector_Simple_Put_Get(t *testing.T) {
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet, err := nvi.GetVector(govector.Vector{0, 0}, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestNaiveThroughput_test(t *testing.T) {
	VectorSize := uint32(10)
	resultSize := uint32(30)
	originalFileItem := uint32(10)
	testFileItem := uint32(10)

	// initiate database
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vecArr[i], chunkPosition)
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
			resultArr, err := nvi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("naive_knn", originalFileItem, testFileItem, putTime, getTime)
}

var originalFileItem = uint32(10000)
var testFileItem = uint32(10000)

func TestNaiveThroughput_test_10(t *testing.T) {
	VectorSize := uint32(10)
	resultSize := uint32(30)

	// initiate database
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vecArr[i], chunkPosition)
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
			resultArr, err := nvi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("naive_knn_10", originalFileItem, testFileItem, putTime, getTime)
}

func TestNaiveThroughput_test_50(t *testing.T) {
	VectorSize := uint32(50)
	resultSize := uint32(30)

	// initiate database
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vecArr[i], chunkPosition)
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
			resultArr, err := nvi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("naive_knn_50", originalFileItem, testFileItem, putTime, getTime)
}

func TestNaiveThroughput_test_100(t *testing.T) {
	VectorSize := uint32(100)
	resultSize := uint32(30)

	// initiate database
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vecArr[i], chunkPosition)
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
			resultArr, err := nvi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("naive_knn_100", originalFileItem, testFileItem, putTime, getTime)
}

func TestNaiveThroughput_test_500(t *testing.T) {
	VectorSize := uint32(500)
	resultSize := uint32(30)

	// initiate database
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vecArr[i], chunkPosition)
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
			resultArr, err := nvi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("naive_knn_500", originalFileItem, testFileItem, putTime, getTime)

}

func TestNaiveThroughput_test_1000(t *testing.T) {
	VectorSize := uint32(1000)
	resultSize := uint32(30)

	// initiate database
	nvi := newNaiveVectorIndex()
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
		_, err := nvi.PutVector(vecArr[i], chunkPosition)
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
			resultArr, err := nvi.GetVector(key, resultSize)
			if err != nil {
				err := fmt.Errorf("get failed: %v", err.Error())
				fmt.Println(err.Error())
			}
			fmt.Println(resultArr)
		}(testArr[i])
	}
	wg.Wait()
	getTime := time.Since(now)
	printReport("naive_knn_1000", originalFileItem, testFileItem, putTime, getTime)
}
