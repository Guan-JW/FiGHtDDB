package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	pb "github.com/FiGHtDDB/comm"
	"github.com/FiGHtDDB/storage"
	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/optimizer"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedDataBaseServer
	db *storage.Db
}

func (s *Server) SendSql(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error) {
	fmt.Println(in.SqlStr)
	txnId := storage.GetTid()
	planTree := parser.Parse(in.SqlStr, txnId)
	planTree.Analyze()
	planTree = optimizer.Optimize(planTree)

	// TODO: execute planTree
	return &pb.SqlResult{Rc: 0, Data: in.SqlStr}, nil
}

func (s *Server) ExecSql(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error) {
	rc := s.db.ExecSql(in.SqlStr)
	
	return &pb.SqlResult{Rc: int32(rc), Data: ""}, nil
}

func (server *Server) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	if storage.ServerName() == "main" {
		storage.ResetTid()
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", storage.ServerPort()))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterDataBaseServer(s, server)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewServer() (*Server, error) {
	s := &Server{}
	s.db = storage.NewDb(storage.GetLocalDbConnStr())
	
	return s, nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("please specify servername. port")
		return
	}

	// load configuration
	serverName := os.Args[1]
	storage.LoadConfig(serverName)

	// start server
	var wg sync.WaitGroup
	server, err := NewServer()
	if err != nil {
		log.Fatal("fail to start server")
		return
	}
	go server.Run(&wg)
	wg.Add(1)

	wg.Wait()
}
