package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pb "github.com/FiGHtDDB/comm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = "localhost:5600"
)

func main() {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewDataBaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// hard-cord queries
	var queries [15]string
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
	queries[11] = `
	select * 
	from publisher 
	where publisher.nation = 'USA'
	`

	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		text = text[:len(text)-1]
		if text == "q" {
			break
		}
		id, err := strconv.Atoi(text)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if id < 0 || id > 11 {
			fmt.Println("id should be in the range[0,10]")
			continue
		}
		// fmt.Println("ready")
		r, err := c.SendSql(ctx, &pb.SqlRequest{SqlStr: queries[id]})
		// fmt.Println("done")
		if err != nil {
			log.Fatal("failed to parse: ", err)
		}
		fmt.Println(r.Rc)
		fmt.Println(r.Data)
		// printTree(planTree)
	}
}
