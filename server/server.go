package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	pb "github.com/FiGHtDDB/comm"
	"github.com/FiGHtDDB/executor"
	"github.com/FiGHtDDB/parser"
	"google.golang.org/grpc"
)

var (
	port = 5556
)

type Server struct {
	pb.UnimplementedDataBaseServer
}

func (s *Server) SendSql(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error){
	planTree := parser.Parse(in.SqlStr)
	resp := make([]byte,0)
	
	rc := executor.Execute(planTree, &resp)
	return &pb.SqlResult{Rc: rc, Data: string(resp)}, nil
}

func (server *Server) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
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
		log.Fatal("please specify servername")
		return
	}

	executor.ServerName = os.Args[1]

	var wg sync.WaitGroup
	// start server
	server, err := NewServer("")
	if err != nil {
		log.Fatal("fail to start server")
		return
	}
	go server.Run(&wg)
	wg.Add(1)

	wg.Wait()
}