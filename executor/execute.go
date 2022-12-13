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
		// fmt.Println("left:", leftNode.TmpTable)
		executeNode(leftNode, tree, resp1)
	}

	if node.Right != -1 {
		rightNode := tree.Nodes[node.Right]
		var resp2 Tuples
		// fmt.Println("right:", rightNode.TmpTable)
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
	//清理main的tmptable
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, _ := sql.Open("postgres", connStr)
	nodeType := node.NodeType
	siteName := node.Locate
	ServerName := storage.ServerName()

	if nodeType != 1 || (nodeType == 1 && siteName != ServerName) {
		tablename := node.TmpTable

		// TODO: assert(plan_node.Right = -1)
		fmt.Println("main drop table:", tablename)
		sqlStr := "drop table if exists " + tablename

		stmt, _ := client.Prepare(sqlStr) //err
		res, _ := stmt.Exec()             //err
		println(res)

		// println(res)
	}
}

func executeSP(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("scan client:", err)

	var sqlStr string
	tableName := tree.Nodes[node.Left].TmpTable
	if node.ExecStmtCols == "" {
		sqlStr = "create table " + node.TmpTable + " as select * from " + tableName
	} else {

		sqlStr = "create table " + node.TmpTable + " as select " + node.ExecStmtCols + " from " + tableName

	}

	ServerName := storage.ServerName()
	if node.Locate == ServerName {
		// fmt.Println("SP main")
		// leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)

		// getTmpTable(tree.Nodes[node.Left], leftAddr)

		// leftAddress := storage.GetServerAddress(tree.Nodes[node.Left].Locate)

		// if tree.Nodes[node.Left].NodeType != 1 {
		// 	res1 := storage.ExecRemoteSql("drop table if exists "+tableName+";", leftAddress)
		// 	fmt.Println(tree.Nodes[node.Left].Locate, "left drop", res1)

		// }

		// fmt.Println("SP:", sqlStr)
		stmt, err := client.Prepare(sqlStr) //err
		fmt.Println("SP prepare:", err)
		res, err := stmt.Exec() //err
		fmt.Println("SP exec:", err)
		println(res)

		// fmt.Println("main will drop", tree.Nodes[node.Left].TmpTable)
		CleanTmpTable(tree.Nodes[node.Left])

	} else {
		// fmt.Println("SP not main")
		address := storage.GetServerAddress(node.Locate)
		leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)

		getRemoteTmpTable(tree.Nodes[node.Left], leftAddr, address)

		if tree.Nodes[node.Left].NodeType != 1 && tree.Nodes[node.Left].Left != -1 {
			res1 := storage.ExecRemoteSql("drop table if exiss "+tableName+";", leftAddr)
			fmt.Println(tree.Nodes[node.Left].Locate, "left drop", res1)
		}

		res := storage.ExecRemoteSql(sqlStr, address)
		fmt.Println(res)

		res3 := storage.ExecRemoteSql("drop table if exiss "+tableName+";", address)
		fmt.Println(node.Locate, "left drop", res3)

	}

}

func executeScan(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("scan client:", err)

	var sqlStr string
	tableName := tree.Nodes[node.Left].TmpTable

	sqlStr = "create table " + node.TmpTable + " as select * from " + tableName + " ;"
	ServerName := storage.ServerName()
	if node.Locate == ServerName {
		leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)

		getTmpTable(tree.Nodes[node.Left], leftAddr)

		// leftAddress := storage.GetServerAddress(tree.Nodes[node.Left].Locate)

		// res1 := storage.ExecRemoteSql("drop table if exists "+tableName+";", leftAddress)
		// fmt.Println("left drop", res1)

		// fmt.Println("sel:", sqlStr)
		stmt, err := client.Prepare(sqlStr) //err
		fmt.Println("sel prepare:", err)
		res, err := stmt.Exec() //err
		fmt.Println("sel exec:", err)
		println(res)

		// CleanTmpTable(tree.Nodes[node.Left])

	} else {
		// fmt.Println(node.Locate, "child:", tree.Nodes[node.Left].Locate)

		address := storage.GetServerAddress(node.Locate)
		// fmt.Println(sqlStr, address)
		// leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)

		// getRemoteTmpTable(tree.Nodes[node.Left], address, address)

		// res1 := storage.ExecRemoteSql("drop table if exiss "+tableName+";", address)
		// fmt.Println("left drop", res1)

		res := storage.ExecRemoteSql(sqlStr, address)
		fmt.Println("scan exec remote", res)

		// res3 := storage.ExecRemoteSql("drop table if exist "+tableName+";", address)
		// fmt.Println("left drop", res3)

	}

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

	ServerName := storage.ServerName()
	if node.Locate == ServerName {
		leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)
		rightAddr := storage.GetServerAddress(tree.Nodes[node.Right].Locate)
		if tree.Nodes[node.Left].Locate != ServerName {

			getTmpTable(tree.Nodes[node.Left], leftAddr)
			res1 := storage.ExecRemoteSql("drop table if exists "+leftTableName+";", leftAddr)
			fmt.Println("left drop", res1)
		}

		if tree.Nodes[node.Right].Locate != ServerName {
			getTmpTable(tree.Nodes[node.Right], rightAddr)

			res2 := storage.ExecRemoteSql("drop table if exists "+rightTableName+";", rightAddr)
			fmt.Println("right drop", res2)

		}

		fmt.Println("union:", sqlStr)
		stmt, err := client.Prepare(sqlStr) //err
		fmt.Println("union prepare:", err)
		res, err := stmt.Exec() //err
		fmt.Println("union exec:", err)
		println(res)

		// fmt.Println("main will drop", tree.Nodes[node.Left].TmpTable)
		CleanTmpTable(tree.Nodes[node.Left])
		// fmt.Println("main will drop", tree.Nodes[node.Right].TmpTable)
		CleanTmpTable(tree.Nodes[node.Right])
	} else {
		address := storage.GetServerAddress(node.Locate)
		leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)
		rightAddr := storage.GetServerAddress(tree.Nodes[node.Right].Locate)
		if tree.Nodes[node.Left].NodeType != 1 || tree.Nodes[node.Left].Locate != node.Locate {
			getRemoteTmpTable(tree.Nodes[node.Left], leftAddr, address)
			res1 := storage.ExecRemoteSql("drop table if exiss "+leftTableName+";", leftAddr)
			fmt.Println(tree.Nodes[node.Left].Locate, "left drop", res1)
		}

		if tree.Nodes[node.Right].NodeType != 1 || tree.Nodes[node.Right].Locate != node.Locate {
			getRemoteTmpTable(tree.Nodes[node.Right], rightAddr, address)

			res2 := storage.ExecRemoteSql("drop table if exists "+rightTableName+";", rightAddr)
			fmt.Println(tree.Nodes[node.Right].Locate, "right drop", res2)
		}

		res := storage.ExecRemoteSql(sqlStr, address)
		fmt.Println(res)

		res3 := storage.ExecRemoteSql("drop table if exists "+leftTableName+";", address)
		fmt.Println(node.Locate, "left drop", res3)
		res4 := storage.ExecRemoteSql("drop table if exists "+rightTableName+";", address)
		fmt.Println(node.Locate, "right drop", res4)
	}

}
func getTmpTable(node parser.PlanTreeNode, address string) {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)

	tableName := node.TmpTable
	// fmt.Println("tmpaddr", address)
	tableName = strings.ToLower(tableName)
	CreateSql := storage.GetRemoteSchema(tableName, address)

	CreateSql = strings.Replace(CreateSql, "integer(32)", "integer", -1)
	CreateSql = strings.Replace(CreateSql, "integer(64)", "integer", -1)
	CreateSql = strings.Replace(CreateSql, ", );", " );", -1)
	// fmt.Println("createsql", CreateSql, address)

	stmt, err := client.Prepare(CreateSql)

	fmt.Println("tmpcreate prepare", err)
	res, err := stmt.Exec()
	fmt.Println("tmp create", res, err)

	insertQuery := "insert into " + tableName + " values "
	query := "select * from " + tableName
	insertPlus := storage.ExecRemoteSelect(query, address)

	insertQuery += insertPlus + ";"

	stmt2, err := client.Prepare(insertQuery)
	fmt.Println("tmpinsert prepare", err)
	res2, err := stmt2.Exec()
	fmt.Println("tmp insert", res2, err)

}

func getRemoteTmpTable(node parser.PlanTreeNode, address string, dest string) {

	tableName := node.TmpTable

	tableName = strings.ToLower(tableName)
	CreateSql := storage.GetRemoteSchema(tableName, address)

	CreateSql = strings.Replace(CreateSql, "integer(32)", "integer", -1)
	CreateSql = strings.Replace(CreateSql, "integer(64)", "integer", -1)
	CreateSql = strings.Replace(CreateSql, ", );", " );", -1)

	res := storage.ExecRemoteSql(CreateSql, dest)
	fmt.Println("tmp,", res)

	insertQuery := "insert into " + tableName + " values "
	query := "select * from " + tableName
	insertPlus := storage.ExecRemoteSelect(query, address)
	insertQuery += insertPlus + ";"
	res2 := storage.ExecRemoteSql(query, dest)
	fmt.Println("tmp insert", res2)

}
func executeJoin(node parser.PlanTreeNode, tree *parser.PlanTree, resp Tuples) {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("join client:", err)

	var sqlStr string
	leftTableName := tree.Nodes[node.Left].TmpTable
	rightTableName := tree.Nodes[node.Right].TmpTable

	print(node.ExecStmtWhere, node.Where)
	if node.ExecStmtWhere == "" {
		sqlStr = "create table " + node.TmpTable + " as select * from " + leftTableName + " natural join " + rightTableName + ";"

	} else {
		sqlStr = "create table " + node.TmpTable + " as select * from " + leftTableName + " natural join " + rightTableName + " " + node.ExecStmtWhere + ";"

	}

	ServerName := storage.ServerName()
	if node.Locate == ServerName {
		leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)
		rightAddr := storage.GetServerAddress(tree.Nodes[node.Right].Locate)
		// fmt.Println("join addr:", leftAddr, rightAddr)
		if tree.Nodes[node.Left].Locate != ServerName {

			getTmpTable(tree.Nodes[node.Left], leftAddr)
			res1 := storage.ExecRemoteSql("drop table if exists "+leftTableName+";", leftAddr)
			fmt.Println("left drop", res1)
		}
		if tree.Nodes[node.Right].Locate != ServerName {

			getTmpTable(tree.Nodes[node.Right], rightAddr)
			res2 := storage.ExecRemoteSql("drop table if exists "+rightTableName+";", rightAddr)
			fmt.Println("right drop", res2)

		}

		// fmt.Println("join:", sqlStr)
		stmt, err := client.Prepare(sqlStr) //err
		fmt.Println("join prepare:", err)
		res, err := stmt.Exec() //err
		fmt.Println("join exec:", err)
		println(res)

		// fmt.Println("main will drop", tree.Nodes[node.Left].TmpTable)
		CleanTmpTable(tree.Nodes[node.Left])
		// fmt.Println("main will drop", tree.Nodes[node.Right].TmpTable)
		CleanTmpTable(tree.Nodes[node.Right])
	} else {

		address := storage.GetServerAddress(node.Locate)
		leftAddr := storage.GetServerAddress(tree.Nodes[node.Left].Locate)
		rightAddr := storage.GetServerAddress(tree.Nodes[node.Right].Locate)
		// fmt.Println("join addr:", address, leftAddr, rightAddr)
		if tree.Nodes[node.Left].NodeType != 1 || tree.Nodes[node.Left].Locate != node.Locate {
			getRemoteTmpTable(tree.Nodes[node.Left], leftAddr, address)
			res1 := storage.ExecRemoteSql("drop table if exiss "+leftTableName+";", leftAddr)
			fmt.Println(tree.Nodes[node.Left].Locate, "left drop", res1)
		}

		getRemoteTmpTable(tree.Nodes[node.Right], rightAddr, address)

		if tree.Nodes[node.Right].NodeType != 1 || tree.Nodes[node.Right].Locate != node.Locate {

			res2 := storage.ExecRemoteSql("drop table if exists "+rightTableName+";", leftAddr)
			fmt.Println(tree.Nodes[node.Right].Locate, "right drop", res2)
		}

		res := storage.ExecRemoteSql(sqlStr, address)
		fmt.Println(res)

		res3 := storage.ExecRemoteSql("drop table if exists "+leftTableName+";", address)
		fmt.Println(node.Locate, "left drop", res3)
		res4 := storage.ExecRemoteSql("drop table if exists "+rightTableName+";", address)
		fmt.Println(node.Locate, "right drop", res4)
	}

	// client.Close()
}

func generateCreateQuery(node parser.PlanTreeNode) string {
	//连接数据库
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, err := sql.Open("postgres", connStr)
	fmt.Println("create client:", err)

	var create_sql sql.NullString
	query := "select showcreatetable('public','table_name');"
	query = strings.Replace(query, "table_name", node.TmpTable, -1)
	fmt.Println("create query", query)

	rows, err := client.Query(query)
	fmt.Println("create query:", err)
	rows.Next()
	err = rows.Scan(&create_sql)
	fmt.Println("createScan err:", err)
	fmt.Println(create_sql.String)
	createSql := create_sql.String
	createSql = strings.Replace(createSql, "integer(32)", "integer", -1)
	createSql = strings.Replace(createSql, "integer(64)", "integer", -1)
	createSql = strings.Replace(createSql, ", );", " );", -1)
	createSql = createSql[1:]
	fmt.Println(createSql)

	return createSql
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

// select * from
// ExecRemoteSelect(select ) Ex
// ExecGetTable("")
func executeRemoteCreateStmt(address string, create_sql string) {
	// fmt.Println("executeRemoteCreateStmt")
	res := storage.ExecRemoteSql(create_sql, address)
	fmt.Println("exec remote", res)
}
func executeTrans(node parser.PlanTreeNode) {
	ServerName := storage.ServerName()
	// ServerName := "main"
	fmt.Println("servername", ServerName)
	fmt.Println("nodeserver", node.Locate)
	if node.Locate != ServerName {
		fmt.Println("node locate", node.Locate)
		address := storage.GetServerAddress(node.Locate)
		// address := node.Locate + ":" + node.Dest
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
		executeSP(node, tree, resp)

	} else if node.NodeType == 4 { //strings.HasPrefix(node.NodeType, "join") {
		executeJoin(node, tree, resp)

	} else if node.NodeType == 5 { //"union" {
		executeUnion(node, tree, resp)

	} else if node.NodeType == 1 && node.Left != -1 {
		executeScan(node, tree, resp)
	}
	// executeTrans(node)

}

func printResult(tree *parser.PlanTree) string {
	connStr := fmt.Sprintf("dbname=%s port=%d user=%s password=%s sslmode=%s", db.dbname, db.port, db.user, db.password, db.sslmode)
	client, _ := sql.Open("postgres", connStr)

	// result := make([]string, 0)
	var result string
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
		var res string
		// todo: 只插入前100条，之后需要修改
		if i > 10 {
			break
		}
		// todo: 只插入前100条，之后需要修改
		_ = rows.Scan(values...)

		// fmt.Print("|")
		res += "|"
		for j := range values {

			value := reflect.ValueOf(values[j]).Elem().Interface()
			// fmt.Print(Strval(value))
			res += Strval(value)
			// fmt.Print("|")
			res += "|"
		}
		// fmt.Println(" ")
		res += " "
		result += res + "\n"
		// result = append(result, res)
		i++
	}
	CleanTmpTable(node)
	return result
}
func printTree(node parser.PlanTreeNode, tree *parser.PlanTree, num int32) {
	fmt.Println(node.TmpTable)
	if node.Left != -1 {
		leftNode := tree.Nodes[node.Left]
		fmt.Println("left", leftNode.TmpTable)
		printTree(leftNode, tree, num+1)
	} else {
		fmt.Println("no left node")
	}

	if node.Right != -1 {
		rightNode := tree.Nodes[node.Left]
		fmt.Println("right", rightNode.TmpTable)
		printTree(rightNode, tree, num+1)
	} else {
		fmt.Println("no right node")
	}

}
func Execute(tree *parser.PlanTree) string {
	// printTree(tree.Nodes[tree.Root], tree, 0)
	// tree.Print()

	address := storage.GetServerAddress("segment1")
	res := storage.ExecRemoteSql("drop table if exists "+"_transaction_0_tmptable_5"+" ;", address)
	fmt.Println("mainres:", res)
	var resp Tuples

	executeNode(tree.Nodes[tree.Root], tree, resp)
	result := printResult(tree)
	// result := make([]string, 0)
	// result = append(result, "abc")

	return result
}
