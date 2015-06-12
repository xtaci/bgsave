package main

import (
	"github.com/fzzy/radix/extra/cluster"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/vmihailenco/msgpack.v2"
	pb "proto"
	"testing"
)

const (
	address  = "localhost:50004"
	test_key = "testing:3721"
)

type TestStruct struct {
	Id    int32
	Name  string
	Sex   int
	Int64 int64
	F64   float64
	F32   float32
	Data  []byte
}

func TestBgSave(t *testing.T) {
	//t.Skip()
	// start connection to redis cluster
	client, err := cluster.NewCluster(DEFAULT_REDIS_HOST)
	if err != nil {
		t.Fatal(err)
	}

	// mset data into redis
	bin, _ := msgpack.Marshal(&TestStruct{3721, "hello", 18, 999, 1.1, 2.2, []byte("world")})
	reply := client.Cmd("set", test_key, bin)
	if reply.Err != nil {
		t.Fatal(reply.Err)
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(address)
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewBgSaveServiceClient(conn)

	// Contact the server and print out its response.
	_, err = c.MarkDirty(context.Background(), &pb.BgSave_Key{Name: test_key})
	if err != nil {
		t.Fatalf("could not query: %v", err)
	}
}
