package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	pb "github.com/FiGHtDDB/comm"
	"github.com/FiGHtDDB/util"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func ExecRemoteSql(sqlStr string, addr string) int {

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return 2
	}
	defer conn.Close()

	c := pb.NewDataBaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	r, err := c.ExecSql(ctx, &pb.SqlRequest{SqlStr: sqlStr})

	if err != nil {
		log.Fatal("failed to parse: ", err)
		return 1
	}

	return int(r.Rc)
}

func (db *Db) ExecSql(sqlStr string) int {

	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println(err)
		return -1
	}
	defer client.Close()

	res, err := client.Exec(sqlStr)
	if err != nil {
		log.Println(err)
		return -1
	}

	num, err := res.RowsAffected()
	if err != nil {
		log.Println(err)
		return -1
	}

	return int(num)
}
func ExecRemoteSelect(sqlStr string, addr string) string {

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return "2"
	}
	defer conn.Close()

	c := pb.NewDataBaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	r, err := c.ExecSelect(ctx, &pb.SqlRequest{SqlStr: sqlStr})
	if err != nil {
		log.Fatal("failed to parse: ", err)
		return "1"
	}

	return r.Data
}
func (db *Db) ExecSelect(sqlStr string) (string, int) {

	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println(err)
		return "", -1
	}
	defer client.Close()

	rows, err := client.Query(sqlStr)
	if err != nil {
		log.Fatal(err)
		return "", -1
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Println(err)
		return "", -1
	}
	values := make([]interface{}, len(colTypes))
	for i := range values {
		values[i] = reflect.New(colTypes[i].ScanType()).Interface()
	}

	var resStr string
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			log.Println("failed to scan row: ", err)
			return "", -1
		}

		resStr += "("
		for _, value := range values {
			resStr += util.Strval(reflect.ValueOf(value).Elem().Interface()) + ","
		}
		resStr = resStr[:len(resStr)-1]
		resStr += "),"
	}
	resStr = resStr[:len(resStr)-1]

	return resStr, 0
}

func GetRemoteSchema(sqlStr string, addr string) string {

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return "2"
	}
	defer conn.Close()

	c := pb.NewDataBaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	r, err := c.GetSchema(ctx, &pb.SqlRequest{SqlStr: sqlStr})

	if err != nil {
		log.Fatal("failed to parse: ", err)
		return "1"
	}

	return r.Data
}

func (db *Db) GetSchema(tableName string) (string, int) {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println(err)
		return "", -1
	}
	defer client.Close()

	query := "select showcreatetable('public','table_name');"
	query = strings.Replace(query, "table_name", tableName, -1)

	res, err := client.Query(query)
	if err != nil {
		return "", -1
	}
	defer res.Close()

	var str sql.NullString
	for res.Next() {
		res.Scan(&str)
	}

	return str.String, 0
}

func GetTableCount(tableName string, siteName string) int {
	db := configs.DbMetas[siteName]
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.DbName, db.Port, db.User, db.Password, db.Sslmode)
	client, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println(err)
		return 0
	}
	defer client.Close()

	query := fmt.Sprintf("select count(*) from %s;", tableName)
	res, err := client.Query(query)
	if err != nil {
		return 0
	}
	defer res.Close()

	cnt := 0
	for res.Next() {
		res.Scan(&cnt)
	}

	return cnt
}
