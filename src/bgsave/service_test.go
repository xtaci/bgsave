package main

import (
	"github.com/fzzy/radix/redis"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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
	// start connection to redis
	client, err := redis.Dial("tcp", DEFAULT_REDIS_HOST)
	if err != nil {
		t.Fatal(err)
	}

	// mset data into redis
	bin, _ := bson.Marshal(&TestStruct{3721, "hello", 18, 999, 1.1, 2.2, []byte("world")})
	reply := client.Cmd("set", test_key, bin)
	if reply.Err != nil {
		t.Fatal(reply.Err)
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(address)
	if err != nil {
		t.Fatal("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewBgSaveServiceClient(conn)

	// Contact the server and print out its response.
	_, err = c.MarkDirty(context.Background(), &pb.BgSave_Key{Name: test_key})
	if err != nil {
		t.Fatalf("could not query: %v", err)
	}
}
func TestWriteBgSave(t *testing.T) {
	// start connection to mongodb
	sess, err := mgo.Dial(DEFAULT_MONGODB_URL)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	// database is provided in url
	db := sess.DB("test")
	tt := &TestStruct{3721, "hello", 18, 999, 1.1, 2.2, []byte("world")}
	_, err = db.C("testing").Upsert(bson.M{"id": tt.Id}, tt)
	if err != nil {
		t.Fatal(err)
	}
	// get data from mongodb
	t.Log("Save to mongodb: ", tt)
}

func TestReadBgSave(t *testing.T) {
	// start connection to mongodb
	sess, err := mgo.Dial(DEFAULT_MONGODB_URL)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	// database is provided in url
	db := sess.DB("test")
	tt := &TestStruct{}
	err = db.C("testing").Find(bson.M{"id": 3721}).One(&tt)
	if err != nil {
		t.Fatal(err)
	}
	// get data from mongodb
	t.Log("Read from mongodb: ", tt)
}
