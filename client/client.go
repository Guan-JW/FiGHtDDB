package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
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
	var queries [30]string

	queries[0] = `
	create database test;`

	queries[1] = `
	create table publisher (id int key, name char(100), nation char(3))
	horizontal fragmentation (
		(id < 104000 AND nation='PRC') on site main,
		(id < 104000 AND nation='USA') on site segment1,
		(id >= 104000 AND nation='PRC') on site segment2,
		(id >= 104000 AND nation='USA') on site segment3
	);`

	queries[2] = `
	create table book (id int key, title char(100), authors char(200), publisher_id int, copies int)
	horizontal fragmentation (
		(id < 205000) on site main,
		(id >= 205000 AND id < 210000) on site segment1,
		(id >= 210000) on site segment2
	);`

	queries[3] = `
	create table customer (id int key, name char (25), rank int)
	vertical fragmentation (
		(id, name) on site main,
		(id, rank) on site segment1
	);`

	queries[4] = `
	create table orders (customer_id int, book_id int, quantity int)
	horizontal fragmentation (
		(customer_id < 307000 and book_id < 215000) on site main,
		(customer_id < 307000 and book_id >= 215000) on site segment1,
		(customer_id >= 307000 and book_id < 215000) on site segment2,
		(customer_id >= 307000 and book_id >= 215000) on site segment3
	);`

	queries[5] = `
	insert into customer(id, name, rank) 
	values(900001, 'Xiaoming', 1);`

	queries[6] = `
	insert into publisher(id, name, nation) 
	values(104001,'High Education Press', 'PRC');`

	queries[7] = `
	Delete from publisher;`

	queries[8] = `
	delete from customer;`

	queries[9] = `
	select publisher.name
	from publisher;`

	queries[10] = `
	select *
	from customer;`

	queries[11] = `
	select book.title
	from book
	where copies>7000;`

	queries[12] = `
	select customer_id, book_id
	from orders;`

	queries[13] = `
	select book.title, book.copies, publisher.name, publisher.nation 
	from book,publisher 
	where book.publisher_id=publisher.id and 
	publisher.nation='PRC'and book.copies>1000;`

	queries[14] = `
	select customer.name, book.title, publisher.name, orders.quantity 
	from customer,book,publisher,orders 
	where customer.id=orders.customer_id 
	and book.id=orders.book_id and book.publisher_id=publisher.id 
	and book.id>210000 and publisher.nation='PRC' 
	and orders.customer_id >= 307000 and orders.book_id < 215000;`

	queries[15] = `
	select customer.name, customer.rank, orders.quantity
	from customer,orders
	where customer.id=orders.customer_id
	and customer.rank=1;`

	queries[16] = `
	select customer.name, orders.quantity, book.title
	from customer,orders,book
	where customer.id=orders.customer_id
	and book.id=orders.book_id
	and customer.rank=1
	and book.copies>5000;`

	queries[17] = `
	select customer.name, book.title, publisher.name, orders.quantity
	from customer, book, publisher, orders
	where customer.id=orders.customer_id
	and book.id=orders.book_id
	and book.publisher_id=publisher.id
	and book.id>220000
	and publisher.nation='USA'
	and orders.quantity>1;`

	queries[18] = `
	select customer.name, book.title, publisher.name, orders.quantity
	from customer, book, publisher, orders
	where customer.id=orders.customer_id
	and book.id=orders.book_id
	and book.publisher_id=publisher.id
	and customer.id>308000
	and book.copies>100
	and orders.quantity>1
	and publisher.nation='PRC';`

	// queries[10] = `
	// select customer.name, book.title, publisher.name, orders.quantity
	// from customer, book, publisher, orders
	// where customer.id=orders.customer_id
	// and book.id=orders.book_id
	// and book.publisher_id=publisher.id
	// and book.id > 207000
	// and book.id < 213000
	// and book.copies>100
	// and orders.quantity>1
	// and publisher.nation='PRC'

	queries[19] = `
	select * from customer where customer.rank=1;
	`

	queries[20] = `
	insert into book (id, title, authors, publisher_id, copies) 
	values(205001, 'DDB', 'Oszu', 104001, 100);`

	queries[21] = `
	insert into orders (customer_id, book_id, quantity) 
	values(300001, 205001,5);`

	queries[22] = `
	delete from publisher 
	where nation = 'PRC';`

	queries[23] = `delete from customer 
	where name='J. Stephenson' AND rank=3;`

	queries[24] = `
	delete from customer 
	where rank = 1;`

	queries[25] = `
	drop table customer;`

	queries[26] = `
	select book.title, book.copies, publisher.name, publisher.nation 
	from book,publisher 
	where book.publisher_id=publisher.id and 
	publisher.nation='PRC'and book.copies>1000;
	`

	queries[27] = `
	select customer.name, book.title, publisher.name, orders.quantity 
	from customer,book,publisher,orders 
	where customer.id=orders.customer_id 
	and book.id=orders.book_id and book.publisher_id=publisher.id 
	and book.id>210000 and publisher.nation='PRC' 
	and orders.customer_id >= 307000 and orders.book_id < 215000;`

	queries[28] = `
	show meta;`

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">>>")
		text, _ := reader.ReadString(';')
		// fmt.Println(text)
		text = text[:len(text)-1]
		text = strings.Trim(text, " \n")
		// fmt.Println(text)

		if text == "q" {
			break
		}

		// read a number or string
		id, err := strconv.Atoi(text)
		// fmt.Println(id)
		if err == nil {
			if id < 0 || id > 28 {
				fmt.Println("id should be in the range[0,27]")
				continue
			}
			text = queries[id]
			// fmt.Println(text)
		}

		start := time.Now()
		r, err := c.SendSql(ctx, &pb.SqlRequest{SqlStr: text})
		elapsed := time.Since(start)
		if err != nil {
			log.Fatal("failed to parse: ", err)
		}
		fmt.Println("row number: ", r.Rc)
		fmt.Println(r.Data)
		fmt.Println("total run time: ", elapsed)
	}
}
