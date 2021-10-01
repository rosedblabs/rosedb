package main

import (
	"context"
	"flag"
	"github.com/roseduan/rosedb/cmd/proto"
	"google.golang.org/grpc"
	"log"
	"time"
)

var (
	addr = flag.String("addr", "127.0.0.1:5221", "rosedb grpc server addr")
)

func main() {
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("grpc dial err: %+v", err)
		return
	}
	c := proto.NewRosedbClient(conn)
	rsp, err := c.Set(context.Background(), &proto.SetReq{
		Key: []byte("grpc_test_key"),
		Value: []byte("grpc_value_" + time.Now().String()),
	})
	if err != nil || (rsp != nil && rsp.ErrorMsg != "") {
		log.Printf("Set err: %+v, errorMsg: %s", err, rsp.ErrorMsg)
	}
}
