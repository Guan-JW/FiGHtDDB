package executor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/storage"
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

var (
	ServerIp   = ""
	ServerName = ""
)

type Tuples struct {
	colNames []string
	resp     *[][]byte
}

func Strval(value interface{}) string {
	var key string
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key
}

// return type?
// consider we may project, union and join later
func executeNode(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {

	if node.Left != -1 {
		leftNode := tree.Nodes[node.Left]
		var resp1 Tuples
		fmt.Println("left:", leftNode.TmpTable)
		executeNode(leftNode, tree, resp1)
	}

	if node.Right != -1 {
		rightNode := tree.Nodes[node.Right]
		var resp2 Tuples
		fmt.Println("right:", rightNode.TmpTable)
		executeNode(rightNode, tree, resp2)
	}

	executeOperator(node, tree, resp)

	// handle current node
	// switch node := node.(type) {
	// case *parser.ScanOperator:
	// 	executeScanOperator(node, resp)
	// case *parser.UnionOperator:
	// 	executeUnionOperatpr(node, resp, &resp1, &resp2)
	// default:
	// 	log.Fatal("Unimpletemented node type")
	// }
}

// func executeUnionOperatpr(node *parser.UnionOperator, resp *[]byte, respLeftChild *[]byte, respRightChild *[]byte) {
// 	*resp = append(*resp, *respLeftChild...)
// 	*resp = append(*resp, *respRightChild...)
// }

func CleanTmpTable(node parser.PlanTreeNode) {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, _ := sql.Open("postgres", connStr)
	nodeType := node.NodeType
	siteName := node.Locate
	if nodeType != 1 || siteName != ServerName {
		tablename := node.TmpTable
		// TODO: assert(plan_node.Right = -1)
		sqlStr := "drop table if exists " + tablename

		stmt, _ := client.Prepare(sqlStr) //err
		res, _ := stmt.Exec()             //err
		println(res)

		// println(res)
	}
}

func executeScan(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("scan client:", err)

	var sqlStr string
	tableName := tree.Nodes[node.Left].TmpTable
	if node.Cols == "" {
		sqlStr = "create table " + node.TmpTable + " as select * from " + tableName
	} else {

		sqlStr = "create table " + node.TmpTable + " as select " + node.Cols + " from " + tableName
		// cols := node.colNames
		// selectString := ""
		// for _, c := range cols {
		// 	selectString += c + ","
		// }
		// selectString = selectString[:len(selectString)-1]
		// sqlStr = "create table " + node.TempTable + " select " + selectString + " from " + tableName
	}
	fmt.Println("scan:", sqlStr)
	stmt, err := client.Prepare(sqlStr) //err
	fmt.Println("scan prepare:", err)
	res, err := stmt.Exec() //err
	fmt.Println("scan exec:", err)
	println(res)

	CleanTmpTable(tree.Nodes[node.Left])

}

func executeUnion(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("union client:", err)

	var sqlStr string
	leftTableName := tree.Nodes[node.Left].TmpTable
	rightTableName := tree.Nodes[node.Right].TmpTable
	sqlStr = "create table " + node.TmpTable + " as select * from " + leftTableName + " union all" + " select * from " + rightTableName + ";"
	fmt.Println("union:", sqlStr)
	stmt, err := client.Prepare(sqlStr) //err
	fmt.Println("union prepare:", err)
	res, err := stmt.Exec() //err
	fmt.Println("union exec:", err)
	println(res)

	CleanTmpTable(tree.Nodes[node.Left])
	CleanTmpTable(tree.Nodes[node.Left])

}

func executeJoin(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("join client:", err)

	var sqlStr string
	leftTableName := tree.Nodes[node.Left].TmpTable
	rightTableName := tree.Nodes[node.Right].TmpTable

	if node.Where == "" {
		sqlStr = "create table " + node.TmpTable + " as select * from " + leftTableName + "," + rightTableName + ";"

	} else {
		sqlStr = "create table " + node.TmpTable + " as select * from " + leftTableName + "," + rightTableName + " " + node.Where + ";"

	}

	// if node.NodeType == "join0" {
	// 	sqlStr = "create table " + node.TmpTable + " select * from " + leftTableName + "," + rightTableName + ";"

	// } else if node.NodeType == "join1" {
	// 	sqlStr = "create table " + node.TmpTable + " select * from " + leftTableName + "," + rightTableName + " " + node.Where + ";"

	// } else {
	// 	sqlStr = "create table " + node.TmpTable + " select * from " + leftTableName + " natural join " + rightTableName + " " + node.Where + ";"

	// }

	fmt.Println("join:", sqlStr)
	stmt, err := client.Prepare(sqlStr) //err
	fmt.Println("join prepare:", err)
	res, err := stmt.Exec() //err
	fmt.Println("join exec:", err)
	println(res)

	CleanTmpTable(tree.Nodes[node.Left])
	CleanTmpTable(tree.Nodes[node.Right])

	// client.Close()
}

func generateCreateQuery(node parser.PlanTreeNode) string {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("create client:", err)

	// query := "show create table " + node.TmpTable
	// query := "select * from information_schema.columns where table_name = " + node.TmpTable
	// println(query)
	// fmt.Println(query)
	// rows, err := client.Query(query) //err
	// fmt.Println("create query:", err)
	// rows.Next()
	// var table_name sql.NullString
	var create_sql sql.NullString
	query := "select showcreatetable('public','table_name');"
	query = strings.Replace(query, "table_name", node.TmpTable, -1)
	fmt.Println("create query", query)

	rows, err := client.Query(query)
	fmt.Println("create query:", err)
	rows.Next()
	err = rows.Scan(&create_sql)
	fmt.Println("createScan err:", err)
	fmt.Println(create_sql.String + ";")

	// err = rows.Scan(&table_name, &create_sql) //err

	// fmt.Println("createQuery err:", err)

	// // fmt.Println(create_sql.String + ";")
	// // client.Close()
	// return create_sql.String + ";"
	return create_sql.String + ";"
}

func generateInsertQuery(node parser.PlanTreeNode) ([]string, bool) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("insert client:", err)

	mySlice := make([]string, 0)
	insert_query := "insert into " + node.TmpTable + " values "
	query := "select * from " + node.TmpTable
	// println(query)
	rows, _ := client.Query(query) //err:_

	colTypes, _ := rows.ColumnTypes()

	types := make([]reflect.Type, len(colTypes))
	for i, tp := range colTypes {
		// ScanType
		scanType := tp.ScanType()
		types[i] = scanType
	}
	// fmt.Println(" ")
	values := make([]interface{}, len(colTypes))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}
	i := 0
	for rows.Next() {
		if i%1000 == 0 && i != 0 {
			insert_query = insert_query + ";"
			mySlice = append(mySlice, insert_query)
			insert_query = "insert into " + node.TmpTable + " values "
		} else if i != 0 {
			insert_query = insert_query + ", "
		}
		_ = rows.Scan(values...) //err

		insert_query = insert_query + "("
		for j := range values {
			if j != 0 {
				insert_query = insert_query + ", "
			}
			value := reflect.ValueOf(values[j]).Elem().Interface()
			insert_query = insert_query + Strval(value)
			// fmt.Print(Strval(value))
			// fmt.Print(" ")
		}
		insert_query = insert_query + ")"
		// fmt.Println(" ")
		i++
	}
	insert_query = insert_query + ";"
	mySlice = append(mySlice, insert_query)
	// client.Close()
	if i == 0 {
		return mySlice, false
	} else {
		return mySlice, true
	}
}

func executeRemoteCreateStmt(address string, create_sql string) {
	//ExecuteRemoteCreateStmt
}
func executeTrans(node parser.PlanTreeNode) {
	ServerName := storage.ServerName()
	if node.Locate != ServerName {
		address := node.Locate + ":" + node.Dest
		create_sql := generateCreateQuery(node)
		executeRemoteCreateStmt(address, create_sql)
		insert_query, success := generateInsertQuery(node)

		if success {
			for _, query := range insert_query {
				executeRemoteCreateStmt(address, query)
			}
		}

	}
}

func executeOperator(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	if node.NodeType == 2 || node.NodeType == 3 { //"scan" "projection"{
		executeScan(node, tree, resp)

	} else if node.NodeType == 4 { //strings.HasPrefix(node.NodeType, "join") {
		executeJoin(node, tree, resp)

	} else if node.NodeType == 5 { //"union" {
		executeUnion(node, tree, resp)

	}
	executeTrans(node)

}

//	func executeScanOperator(node *parser.ScanOperator, resp *[]byte) {
//		// judge if this operator executed by this site
//		// if so, connect to pg and get result
//		// else, send select str to correspoding node
//		if ServerName == node.SiteName() {
//			// fetch tuples from local database
//			node.Db().FetchTuples(node.TableName(), resp)
//		} else {
//			// construct sqlStr according to Scan operator and send it to anoter server
//			sqlStr := fmt.Sprintf("select * from %s;", node.TableName())
//			storage.FetchRemoteTuples(sqlStr, node.Site(), resp)
//		}
//	}
func printResult(tree *parser.PlanTree) {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, _ := sql.Open("postgres", connStr)

	node := tree.Nodes[tree.Root]

	query := "select * from " + node.TmpTable
	println(query)
	rows, _ := client.Query(query)

	colTypes, _ := rows.ColumnTypes()

	types := make([]reflect.Type, len(colTypes))
	for i, tp := range colTypes {
		// ScanType
		scanType := tp.ScanType()
		types[i] = scanType
	}
	// fmt.Println(" ")
	values := make([]interface{}, len(colTypes))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}
	i := 0
	for rows.Next() {
		// todo: 只插入前100条，之后需要修改
		if i > 10 {
			break
		}
		// todo: 只插入前100条，之后需要修改
		_ = rows.Scan(values...)

		fmt.Print("|")
		for j := range values {

			value := reflect.ValueOf(values[j]).Elem().Interface()
			fmt.Print(Strval(value))
			fmt.Print("|")
		}
		fmt.Println(" ")
		i++
	}

}
func printTree(node parser.PlanTreeNode, tree *parser.PlanTree, num int32) {
	fmt.Println(node.TmpTable)
	if node.Left != -1 {
		leftNode := tree.Nodes[node.Left]
		fmt.Println("left", leftNode.TmpTable)
		printTree(leftNode, tree, num+1)
	}
	if node.Right != -1 {
		rightNode := tree.Nodes[node.Left]
		fmt.Println("right", rightNode.TmpTable)
		printTree(rightNode, tree, num+1)
	} else {
		fmt.Println("no right node")
	}

}
func Execute(tree *parser.PlanTree) int32 {
	printTree(tree.Nodes[tree.Root], tree, 0)

	var resp Tuples
	executeNode(tree.Nodes[tree.Root], tree, resp)
	printResult(tree)

	return 0
}
