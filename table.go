package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Db struct {
	dbname   string
	user     string
	password string
	port     int
	sslmode  string
}

func NewDb(dbname string, user string, password string, port int, sslmode string) *Db {
	db := new(Db)
	db.dbname = dbname
	db.user = user
	db.password = password
	db.port = port
	db.sslmode = sslmode

	return db
}

var db = NewDb("postgres", "postgres", "postgres", 5700, "disable")

func CleanTmpTable() {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, _ := sql.Open("postgres", connStr)
	tablename := "_transaction_0_tmptable_1"
	// TODO: assert(plan_node.Right = -1)
	sqlStr := "drop table if exists " + tablename

	stmt, err := client.Prepare(sqlStr) //err
	fmt.Println(err)
	res, err := stmt.Exec() //err
	fmt.Println(err)
	println(res)

}

func main() {
	CleanTmpTable()
}
