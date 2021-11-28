package rosedb

import (
	"context"
	"testing"
	"time"
)

func TestTxnMark_Begin(t *testing.T) {
	txnMark := &TxnMark{}
	closed := &closeSignal{
		chn: make(chan struct{}),
	}
	defer close(closed.chn)

	txnMark.Init(closed)
	txnMark.Begin(1)

	go func() {
		err := txnMark.WaitDone(context.Background(), 1)
		if err != nil {
			t.Error(err)
		} else {
			t.Log("finish wait...")
		}
	}()

	go func() {
		time.Sleep(2 * time.Second)
		txnMark.Done(1)
	}()

	time.Sleep(5 * time.Second)
}
