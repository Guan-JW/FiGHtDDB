package executor

import (
	"database/sql"
	"database/sqlStr"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/FiGHtDDB/parser"
)

type Db struct {
	dbname   string
	user     string
	password string
	port     int
	sslmode  string
}

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
func executeNode(node parser.PlanTreeNode, resp Tuples) {
	if node == nil {
		return
	}

	resp1 := make([]byte, 0)
	executeNode(node.Left, &resp1)

	resp2 := make([]byte, 0)
	executeNode(node.Right, &resp2)

	executeOperator(node, resp)

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

func CleanTmpTable(node parser.ScanOperator) {
	nodeType := node.NodeType
	siteName := node.siteName()
	if nodeType != "table" || siteName != ServerName {
		tablename := node.TmpTable()
		// TODO: assert(plan_node.Right = -1)
		sqlStr := "drop table if exists " + tablename

		stmt, _ := client.Prepare(sqlStr) //err
		res, _ := stmt.Exec()             //err
		println(res)

		// println(res)
	}
}

func executeScan(node parser.ScanOperator, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", client.dbname, client.port, client.user, client.password, client.sslmode)
	client, _ := sql.Open("postgres", connStr)

	var sqlStr string
	tableName := node.left.TmpTable
	if node.Cols == nil {
		sqlStr = "create table " + node.TempTable + " select * from " + tableName
	} else {

		sqlStr = "create table " + node.TempTable + " select " + node.Cols + " from " + tableName
		// cols := node.colNames
		// selectString := ""
		// for _, c := range cols {
		// 	selectString += c + ","
		// }
		// selectString = selectString[:len(selectString)-1]
		// sqlStr = "create table " + node.TempTable + " select " + selectString + " from " + tableName
	}
	stmt, _ := client.Prepare(sqlStr) //err
	res, _ := stmt.Exec()             //err
	println(res)

	CleanTmpTable(node.left)

}

func executeUnion(node parser.ScanOperator, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", client.dbname, client.port, client.user, client.password, client.sslmode)
	client, _ := sql.Open("postgres", connStr)

	var sqlStr string
	leftTableName := node.left.TmpTable()
	rightTableName := node.right.TmpTable()
	sqlStr = "create table " + node.TmpTable() + " select * from " + leftTableName + " union all" + " select * from " + rightTableName + ";"
	stmt, _ := client.Prepare(sqlStr) //err
	res, _ := stmt.Exec()             //err
	println(res)

	CleanTmpTable(node.left)
	CleanTmpTable(node.right)

}

func executeJoin(node parser.ScanOperator, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", client.dbname, client.port, client.user, client.password, client.sslmode)
	client, _ := sql.Open("postgres", connStr)

	var sqlStr string
	leftTableName := node.left.TmpTable()
	rightTableName := node.right.TmpTable()

	if node.NodeType == "join0" {
		sqlStr = "create table " + node.TmpTable() + " select * from " + leftTableName + "," + rightTableName + ";"

	} else if node.NodeType == "join1" {
		sqlStr = "create table " + node.TmpTable() + " select * from " + leftTableName + "," + rightTableName + " " + node.Where + ";"

	} else {
		sqlStr = "create table " + node.TmpTable() + " select * from " + leftTableName + " natural join " + rightTableName + " " + node.Where + ";"

	}

	stmt, _ := client.Prepare(sqlStr) //err
	res, _ := stmt.Exec()             //err
	println(res)

	CleanTmpTable(node.left)
	CleanTmpTable(node.right)

	// client.Close()
}

func generateCreateQuery(node *parser.PlanTreeNode) string {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", client.dbname, client.port, client.user, client.password, client.sslmode)
	client, _ := sql.Open("postgres", connStr)

	query := "show create table " + node.TmpTable()
	// println(query)
	rows, _ := client.Query(query) //err
	rows.Next()
	var table_name sqlStr.NullString
	var create_sql sqlStr.NullString
	_ = rows.Scan(&table_name, &create_sql) //err

	// fmt.Println(create_sql.String + ";")
	// client.Close()
	return create_sql.String + ";"
}

func generateInsertQuery(node **parser.PlanTreeNode) ([]string, bool) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", client.dbname, client.port, client.user, client.password, client.sslmode)
	client, _ := sql.Open("postgres", connStr)

	mySlice := make([]string, 0)
	insert_query := "insert into " + node.TmpTable() + " values "
	query := "select * from " + node.TmpTable()
	// println(query)
	rows, _ := client.Query(query) //err:_

	tt, _ := rows.ColumnTypes()

	types := make([]reflect.NodeType, len(tt))
	for i, tp := range tt {
		// ScanType
		scanType := tp.ScanNodeType
		types[i] = scanType
	}
	// fmt.Println(" ")
	values := make([]interface{}, len(tt))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}
	i := 0
	for rows.Next() {
		if i%1000 == 0 && i != 0 {
			insert_query = insert_query + ";"
			mySlice = append(mySlice, insert_query)
			insert_query = "insert into " + node.TmpTable() + " values "
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

func executeCreate(address string, create_sql string) {
	//ExecuteRemoteCreateStmt
}
func executeTrans(node *parser.PlanTreeNode) {
	if node.siteName() != ServerName {
		address := node.ip + ":" + node.port
		create_sql := generateCreateQuery(node)
		executeCreate(address, create_sql)
		insert_query, success := generateInsertQuery(node)

		if success {
			for _, query := range insert_query {
				executeCreate(address, query)
			}
		}

	}
}

func executeOperator(node *parser.PlanTreeNode, resp Tuples) {
	if node.NodeType == 2 || node.NodeType == 3 { //"scan" "projection"{
		executeScan(node, resp)

	} else if node.NodeType == 4 { //strings.HasPrefix(node.NodeType, "join") {
		executeJoin(node, resp)

	} else if node.NodeType == 5 { //"union" {
		executeUnion(node, resp)

	}
	executeTrans(node)

}

// func executeScanOperator(node *parser.ScanOperator, resp *[]byte) {
// 	// judge if this operator executed by this site
// 	// if so, connect to pg and get result
// 	// else, send select str to correspoding node
// 	if ServerName == node.SiteName() {
// 		// fetch tuples from local database
// 		node.Db().FetchTuples(node.TableName(), resp)
// 	} else {
// 		// construct sqlStr according to Scan operator and send it to anoter server
// 		sqlStr := fmt.Sprintf("select * from %s;", node.TableName())
// 		storage.FetchRemoteTuples(sqlStr, node.Site(), resp)
// 	}
// }

func Execute(tree *parser.PlanTree, resp Tuples) int32 {
	executeNode(tree.Root(), resp)

	return 0
}
