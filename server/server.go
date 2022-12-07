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

var (
	port = 5556
)

type Server struct {
	pb.UnimplementedDataBaseServer
}

func (s *Server) SendSql(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error) {
	fmt.Println(in.SqlStr)
	// TODO: get txn id from meta
	var txnId int64 = 0
	planTree := parser.Parse(in.SqlStr, txnId)
	planTree.Analyze()
	planTree = optimizer.Optimize(planTree)

	// TODO: execute planTree
	return &pb.SqlResult{Rc: 0, Data: in.SqlStr}, nil
}

func (s *Server) Scan(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error) {
	// TODO: construct a plan tree with one scan node
	db := storage.NewDb("postgres", "postgres", "postgres", 5700, "disable")
	resp := make([]byte, 0)
	db.FetchTuples("Publisher", &resp)

	return &pb.SqlResult{Rc: 0, Data: string(resp)}, nil
}

func (server *Server) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", storage.ServerPort()))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterDataBaseServer(s, &Server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewServer(cfgPath string) (*Server, error) {
	// TODO: parse config file and construct server
	s := &Server{}
	return s, nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("please specify servername. port")
		return
	}

	// load configuration
	storage.ServerName = os.Args[1]
	storage.LoadConfig()

	// start server
	var wg sync.WaitGroup
	server, err := NewServer("")
	if err != nil {
		log.Fatal("fail to start server")
		return
	}
	go server.Run(&wg)
	wg.Add(1)

	wg.Wait()
}
