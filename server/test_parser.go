package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/FiGHtDDB/comm"
	"github.com/FiGHtDDB/optimizer"
	"github.com/FiGHtDDB/parser"
	"google.golang.org/grpc"
)

var (
	port = 5556
)

type Server struct {
	pb.UnimplementedDataBaseServer
}

func (s *Server) SendSql(ctx context.Context, in *pb.SqlRequest) (*pb.SqlResult, error) {
	// TODO: get txn id from meta
	// txnID := int64(0)
	// planTree := parser.Parse(in.SqlStr, txnID)
	// resp := make([]byte, 0)

	// rc := executor.Execute(planTree, &resp)
	// return &pb.SqlResult{Rc: rc, Data: string(resp)}, nil
	return nil, nil
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

var queries [15]string

func main() {
	queries[0] = `
	select *
	from customer`

	queries[1] = `
	select publisher.name
	from publisher`

	queries[2] = `
	select book.title
	from book
	where copies>5000`

	queries[3] = `
	select orders.customer_id,quantity
	from orders
	where quantity<8`

	queries[4] = `
	select book.title, book.copies, publisher.name, publisher.nation
	from book, publisher
	where book.publisher_id = publisher.id
	and publisher.nation='USA'
	and book.copies>1000`

	queries[5] = `
	select customer.name, orders.quantity
	from customer,orders
	where customer.id=orders.customer_id`

	queries[6] = `
	select customer.name, customer.rank, orders.quantity
	from customer,orders
	where customer.id=orders.customer_id
	and customer.rank=1`

	queries[7] = `
	select customer.name, orders.quantity, book.title
	from customer,orders,book
	where customer.id=orders.customer_id
	and book.id=orders.book_id
	and customer.rank=1
	and book.copies>5000`

	queries[8] = `
	select customer.name, book.title, publisher.name, orders.quantity
	from customer, book, publisher, orders
	where customer.id=orders.customer_id
	and book.id=orders.book_id
	and book.publisher_id=publisher.id
	and book.id>220000
	and publisher.nation='USA'
	and orders.quantity>1`

	queries[9] = `
	select customer.name, book.title, publisher.name, orders.quantity
	from customer, book, publisher, orders
	where customer.id=orders.customer_id
	and book.id=orders.book_id
	and book.publisher_id=publisher.id
	and customer.id>308000
	and book.copies>100
	and orders.quantity>1
	and publisher.nation='PRC'`

	queries[10] = `
	select customer.name, book.title, publisher.name, orders.quantity 
	from customer, book, publisher, orders 
	where customer.id=orders.customer_id 
	and book.id=orders.book_id 
	and book.publisher_id=publisher.id 
	and book.id > 207000 
	and book.id < 213000 
	and book.copies>100 
	and orders.quantity>1 
	and publisher.nation='PRC'
	`

	// // start server
	// server, err := NewServer("")
	// if err != nil {
	// 	log.Fatal("fail to start server")
	// 	return
	// }
	// ctx, _ := context.WithTimeout(context.Background(), time.Minute)
	for i := 10; i <= 10; i++ {
		fmt.Println("******************** ", i, "*********************")
		// server.SendSql(ctx, &pb.SqlRequest{SqlStr: queries[i], i})
		txnID := int64(0)
		planTree := parser.Parse(queries[i], txnID)
		planTree.DrawPlanTree(i, "0")
		planTree.Analyze()
		planTree.DrawPlanTree(i, "1")
		planTree = optimizer.Optimize(planTree)
		planTree.DrawPlanTree(i, "2")
		planTree.DrawPlanTreeTmpTable(i, "tmp")
		// planTree.Print()
	}
}
