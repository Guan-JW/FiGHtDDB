package main

import (
	"fmt"

	"github.com/FiGHtDDB/storage"
)

func storeTableMeta() {
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
}

func main() {
	storeTableMeta()
	// addr := "10.77.50.209:5601"
	// conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	// 	fmt.Println("did not connect: %v", err)
	// }
	// defer conn.Close()

	// c := pb.NewDataBaseClient(conn)
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	// defer cancel()

	// query := "select * from book where copies > 5000;"
	// rc, data := c.ExecSelect(ctx, &pb.SqlRequest{SqlStr: query})
	// fmt.Println("rc: ", rc, "data: ", data)
}
