package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	pb "github.com/FiGHtDDB/comm"
	"github.com/FiGHtDDB/util"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Db struct {
	dbname	string
	user   	string
	password string
	port 	int
	sslmode string
}

func NewDb(dbname string, user string, password string, port int, sslmode string) (*Db) {
	db := new(Db)
	db.dbname = dbname
	db.user = user
	db.password = password
	db.port = port
	db.sslmode = sslmode

	return db
}

func (db *Db) FetchTuples(tableName string, resp *[]byte) {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sqlStr := fmt.Sprintf("select * from %s;", tableName)
	rows, err := client.Query(sqlStr)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer rows.Close()

	// access etcd for definition of table

	for rows.Next() {
		var id int
		var name string
		var nation string
		rows.Scan(&id, &name, &nation)
		err := rows.Err()
		if err != nil {
			log.Fatal("failed to scan row: ", err)
			break
		}
		util.TupleToByte(resp, id, name, nation)
	}
}

func FetchRemoteTuples(sqlStr string, addr string, resp *[]byte) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return
	}
	defer conn.Close()

	c := pb.NewDataBaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r, err := c.Scan(ctx, &pb.SqlRequest{SqlStr: sqlStr})
	if err != nil {
		log.Fatal("failed to parse: ", err)
		return
	}
	*resp = []byte(r.Data)
}