package main

import (
	log "github.com/GameGophers/nsq-logger"
	"google.golang.org/grpc"
	"net"
	pb "proto"
)

const (
	_port = ":50004"
)

func main() {
	// 监听
	lis, err := net.Listen("tcp", _port)
	if err != nil {
		log.Critical(SERVICE, err)
	}
	log.Info(SERVICE, "listening on ", lis.Addr())

	// 注册服务
	s := grpc.NewServer()
	ins := &server{}
	ins.init()
	pb.RegisterBgSaveServiceServer(s, ins)

	// 开始服务
	s.Serve(lis)
}
