package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	pb "github.com/FiGHtDDB/comm"
	"github.com/FiGHtDDB/executor"
	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/storage"
	"google.golang.org/grpc"
)

var (
	port = 5556
)

type Server struct {
	pb.UnimplementedDataBaseServer
}

func (s *Server) SendSql(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error) {
<<<<<<< HEAD
=======
	fmt.Println(in.SqlStr)
>>>>>>> add parser
	planTree := parser.Parse(in.SqlStr)
	resp := make([]byte, 0)

	rc := executor.Execute(planTree, &resp)
	return &pb.SqlResult{Rc: rc, Data: string(resp)}, nil
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
	if len(os.Args) != 3 {
		log.Fatal("please specify servername. port")
		return
	}

	executor.ServerName = os.Args[1]
	var err error
	port, err = strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
		return
	}

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
