package rosedb

// run `bash gen_pb_go.sh` before you run this test
// run grpc server and listen to 127.0.0.1:5221
// just `cd cmd/server && go build main.go && ./main --config ../../config_grpc.toml`
import (
	"context"
	"github.com/roseduan/rosedb/cmd/proto"
	"google.golang.org/grpc"
	"log"
	"testing"
	"time"
)

var (
	c proto.RosedbClient
	conn *grpc.ClientConn = nil
	err error
)
func GRPCSet() error {
	conn, err = grpc.Dial("127.0.0.1:5221", grpc.WithInsecure())
	if err != nil {
		log.Printf("grpc dial err: %+v", err)
		return err
	}
	c = proto.NewRosedbClient(conn)
	return nil
}

func IsInit(t *testing.T) {
	if conn == nil {
		if et := GRPCSet(); et != nil {
			t.Fatalf("error: %+v", et)
		}
	}
}

func TestGRPCSet(t *testing.T) {
	IsInit(t)
	rsp, et := c.Set(context.Background(), &proto.SetReq{
		Key: []byte("test_grpc_set"),
		Value: []byte("test_grpc_set" + time.Now().String()),
	})
	if et != nil {
		t.Error(et)
	}
	if rsp.ErrorMsg != "" {
		t.Errorf("error_msg: %s", rsp.ErrorMsg)
	}
}

func TestGRPCGet(t *testing.T) {
	IsInit(t)
	rsp, et := c.Get(context.Background(), &proto.GetReq{
		Key: []byte("test_grpc_set"),
	})
	if et != nil {
		t.Error(et)
	}
	if rsp.ErrorMsg != "" {
		t.Errorf("error_msg: %s", rsp.ErrorMsg)
	}
	log.Println(string(rsp.Dest))
}