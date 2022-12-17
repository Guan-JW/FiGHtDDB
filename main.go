package main

import (
	"fmt"

	"github.com/FiGHtDDB/optimizer"
	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/storage"
)

func printTree(node parser.PlanTreeNode, tree *parser.PlanTree, num int32) {
	fmt.Println(node.TmpTable)
	if node.Left != -1 {
		leftNode := tree.Nodes[node.Left]
		fmt.Println("left: ", leftNode)
		printTree(leftNode, tree, num+1)
	} else {
		fmt.Println("no left node")
	}

	if node.Right != -1 {
		rightNode := tree.Nodes[node.Right]
		fmt.Println("right: ", rightNode)
		printTree(rightNode, tree, num+1)
	} else {
		fmt.Println("no right node")
	}
}

func main() {
	storage.LoadConfig("main")

	var t storage.TableMeta
	t.TableName = "publisher"
	t.FragNum = 4

	// publisher
	frag := new(storage.FragSchema)
	frag.SiteName = "main"
	frag.Cols = append(frag.Cols, "id", "name", "nation")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: "<", Value: "104000"},
		storage.Condition{Col: "nation", Type: "string", Comp: "=", Value: "'PRC'"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment1"
	frag.Cols = append(frag.Cols, "id", "name", "nation")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: "<", Value: "104000"},
		storage.Condition{Col: "nation", Type: "string", Comp: "=", Value: "'USA'"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment2"
	frag.Cols = append(frag.Cols, "id", "name", "nation")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: ">=", Value: "104000"},
		storage.Condition{Col: "nation", Type: "string", Comp: "=", Value: "'PRC'"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment3"
	frag.Cols = append(frag.Cols, "id", "name", "nation")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: ">=", Value: "104000"},
		storage.Condition{Col: "nation", Type: "string", Comp: "=", Value: "'USA'"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	err := storage.StoreTableMeta(&t)
	if err != nil {
		fmt.Println(err)
		return
	}

	// book
	t.TableName = "book"
	t.FragNum = 3
	t.FragSchema = t.FragSchema[:0]

	frag = new(storage.FragSchema)
	frag.SiteName = "main"
	frag.Cols = append(frag.Cols, "id", "title", "authors", "publisher_id", "copies")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: "<", Value: "205000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment1"
	frag.Cols = append(frag.Cols, "id", "title", "authors", "publisher_id", "copies")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: ">=", Value: "205000"},
		storage.Condition{Col: "id", Type: "int", Comp: "<", Value: "210000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment2"
	frag.Cols = append(frag.Cols, "id", "title", "authors", "publisher_id", "copies")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "id", Type: "int", Comp: ">=", Value: "210000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)
	err = storage.StoreTableMeta(&t)
	if err != nil {
		fmt.Println(err)
		return
	}

	// customer
	t.TableName = "customer"
	t.FragNum = 2
	t.FragSchema = t.FragSchema[:0]

	frag = new(storage.FragSchema)
	frag.SiteName = "main"
	frag.Cols = append(frag.Cols, "id", "name")
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment1"
	frag.Cols = append(frag.Cols, "id", "rank")
	t.FragSchema = append(t.FragSchema, *frag)
	err = storage.StoreTableMeta(&t)
	if err != nil {
		fmt.Println(err)
		return
	}

	// orders
	t.TableName = "orders"
	t.FragNum = 4
	t.FragSchema = t.FragSchema[:0]

	frag = new(storage.FragSchema)
	frag.SiteName = "main"
	frag.Cols = append(frag.Cols, "customer_id", "book_id", "quantity")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "customer_id", Type: "int", Comp: "<", Value: "307000"},
		storage.Condition{Col: "book_id", Type: "int", Comp: "<", Value: "215000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment1"
	frag.Cols = append(frag.Cols, "customer_id", "book_id", "quantity")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "customer_id", Type: "int", Comp: "<", Value: "307000"},
		storage.Condition{Col: "book_id", Type: "int", Comp: ">=", Value: "215000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment2"
	frag.Cols = append(frag.Cols, "customer_id", "book_id", "quantity")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "customer_id", Type: "int", Comp: ">=", Value: "307000"},
		storage.Condition{Col: "book_id", Type: "int", Comp: "<", Value: "215000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)

	frag = new(storage.FragSchema)
	frag.SiteName = "segment3"
	frag.Cols = append(frag.Cols, "customer_id", "book_id", "quantity")
	frag.Conditions = append(frag.Conditions,
		storage.Condition{Col: "customer_id", Type: "int", Comp: ">=", Value: "307000"},
		storage.Condition{Col: "book_id", Type: "int", Comp: ">=", Value: "215000"},
	)
	t.FragSchema = append(t.FragSchema, *frag)
	err = storage.StoreTableMeta(&t)
	if err != nil {
		fmt.Println(err)
		return
	}

	st, err := storage.GetTableMeta("orders")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(st.TableName)
	fmt.Println(st.FragSchema[0].Cols)
	fmt.Println(st.FragSchema[3].Conditions[0].Col)
	fmt.Println(st.FragSchema[1].Conditions[1].Value)
	fmt.Println(st.FragSchema[2].Conditions[0].Comp)

	var queries [25]string
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
	insert into customer(id, name, rank) 
	values(300001, 'Xiaoming', 1);`

	queries[12] = `
	insert into publisher(id, name, nation) 
	values(104001,'High Education Press', 'PRC')`

	queries[13] = `
	insert into customer(id, name, rank) 
	values(300002,'Xiaohong', 1)`

	queries[14] = `
	insert into book (id, title, authors, publisher_id, copies) 
	values(205001, 'DDB', 'Oszu', 104001, 100)`

	queries[15] = `
	insert into orders (customer_id, book_id, quantity) 
	values(300001, 205001,5)`

	queries[16] = `
	delete from orders`

	queries[17] = `
	Delete from book 
	where copies = 100`

	queries[18] = `
	delete from publisher 
	where nation = 'PRC'`

	queries[19] = `delete from customer 
	where name='Xiaohong' AND rank=1`

	queries[20] = `
	delete from customer 
	where rank = 1`

	queries[21] = `
	create table test(id int key, name char(100))`

	for i := 6; i <= 6; i++ {
		fmt.Println("*************", i, "***************")
		var txnId int64 = 0
		planTree := parser.Parse(queries[i], txnId)
		planTree.Analyze()
		planTree.DrawPlanTree(i, "1")
		planTree = optimizer.Optimize(planTree)
		planTree.DrawPlanTree(i, "2")
		planTree.DrawPlanTreeTmpTable(i, "tmp")
		// planTree.Print()
		// if planTree.Root >= 0 && planTree.Nodes[planTree.Root].NodeType != 6 {
		// 	printTree(planTree.Nodes[planTree.Root], planTree, 0)
		// } else {
		// 	fmt.Println("No plan tree available")
		// }
		// fmt.Println(planTree.Nodes[12].TmpTable)
		// os.Exit(0)
	}
}
