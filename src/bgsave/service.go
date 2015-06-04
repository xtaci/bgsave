package main

import (
	log "github.com/GameGophers/nsq-logger"
	"github.com/fzzy/radix/redis"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	pb "proto"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	SERVICE             = "[BGSAVE]"
	DEFAULT_SAVE_DELAY  = 100 * time.Millisecond
	DEFAULT_REDIS_HOST  = "127.0.0.1:6379"
	DEFAULT_MONGODB_URL = "mongodb://127.0.0.1/mydb"
	ENV_REDIS_HOST      = "REDIS_HOST"
	ENV_MONGODB_URL     = "MONGODB_URL"
	ENV_SAVE_DELAY      = "SAVE_DELAY"
	BUFSIZ              = 4096
	BATCH_SIZE          = 1024 // data save batch size
)

type server struct {
	wait        chan string
	redis_host  string
	mongodb_url string
}

func (s *server) init() {
	s.redis_host = DEFAULT_REDIS_HOST
	if env := os.Getenv(ENV_REDIS_HOST); env != "" {
		s.redis_host = env
	}

	s.mongodb_url = DEFAULT_MONGODB_URL
	if env := os.Getenv(ENV_MONGODB_URL); env != "" {
		s.mongodb_url = env
	}

	s.wait = make(chan string, BUFSIZ)
	go s.loader_task()
}

func (s *server) MarkDirty(ctx context.Context, in *pb.BgSave_Key) (*pb.BgSave_NullResult, error) {
	s.wait <- in.Name
	return &pb.BgSave_NullResult{}, nil
}

func (s *server) MarkDirties(ctx context.Context, in *pb.BgSave_Keys) (*pb.BgSave_NullResult, error) {
	for k := range in.Names {
		s.wait <- in.Names[k]
	}
	return &pb.BgSave_NullResult{}, nil
}

// background loader, copy chan into map, execute dump every DEFAULT_SAVE_DELAY
func (s *server) loader_task() {
	for {
		dirty := make(map[string]bool)
		timer := time.After(DEFAULT_SAVE_DELAY)
		select {
		case key := <-s.wait:
			dirty[key] = true
		case <-timer:
			s.dump(dirty)
			dirty = make(map[string]bool)
		}
	}
}

// dump all dirty data into backend database
func (s *server) dump(dirty map[string]bool) {
	// start connection to redis
	client, err := redis.Dial("tcp", s.redis_host)
	if err != nil {
		log.Critical(err)
		return
	}
	defer client.Close()

	// start connection to mongodb
	sess, err := mgo.Dial(s.mongodb_url)
	if err != nil {
		log.Critical(err)
		return
	}
	defer sess.Close()
	// database is provided in url
	db := sess.DB("")

	// copy dirty map into array
	dirty_list := make([]interface{}, 0, len(dirty))
	for k := range dirty {
		dirty_list = append(dirty_list, k)
	}

	if len(dirty_list) == 0 { // ignore emtpy dirty list
		return
	}

	// write data in batch
	var sublist []interface{}
	for i := 0; i < len(dirty_list); i += BATCH_SIZE {
		if (i+1)*BATCH_SIZE > len(dirty_list) { // reach end
			sublist = dirty_list[i*BATCH_SIZE:]
		} else {
			sublist = dirty_list[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
		}

		// mget data from redis
		records, err := client.Cmd("mget", sublist...).ListBytes()
		if err != nil {
			log.Critical(err)
			return
		}

		// save to mongodb
		var tmp map[string]interface{}
		for k, v := range sublist {
			err := bson.Unmarshal(records[k], &tmp)
			if err != nil {
				log.Critical(err)
				continue
			}

			// split key into TABLE NAME and RECORD ID
			strs := strings.Split(v.(string), ":")
			if len(strs) != 2 { // log the wrong key
				log.Critical("cannot split key", v)
				continue
			}
			tblname, id_str := strs[0], strs[1]
			// save data to mongodb
			id, err := strconv.Atoi(id_str)
			if err != nil {
				log.Critical(err)
				continue
			}

			_, err = db.C(tblname).Upsert(bson.M{"Id": id}, tmp)
			if err != nil {
				log.Critical(err)
				continue
			}
		}
	}
	runtime.GC()
}
