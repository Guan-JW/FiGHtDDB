package parser

import (
	"fmt"
	"os"
	"strings"

	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/xwb1989/sqlparser"
)

// CreateTableNode create table node
func CreateTableNode(tableName string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 1
	node.TmpTable = tableName
	return node
}

// CreateSelectionNode create selection nnode
func CreateSelectionNode(TmpTableName string, where string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 2
	node.TmpTable = TmpTableName
	node.Where = where
	return node
}

// CreateProjectionNode create projection node
func CreateProjectionNode(TmpTableName string, cols string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 3
	node.TmpTable = TmpTableName
	node.Cols = cols
	return node
}

// CreateJoinNode create join node
func CreateJoinNode(TmpTableName string, JointType int64) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 4
	node.TmpTable = TmpTableName
	// node.Joint_type = JointType
	return node
}

// CreateUnionNode create union node
func CreateUnionNode(TmpTableName string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 5
	node.TmpTable = TmpTableName
	return node
}

// ResetColsForWhere reset cols to get a unique colname
func ResetColsForWhere(strin string) (strout string) {
	strout = ""
	f := func(c rune) bool {
		if c == ' ' || c == ',' {
			return true
		}
		return false
	}
	arr := strings.FieldsFunc(strin, f)
	for i, str := range arr {
		switch str {
		case "publisher.id":
			arr[i] = "pid"
		case "publisher.name":
			arr[i] = "pname"
		case "publisher.nation":
			arr[i] = "nation"
		case "book.id":
			arr[i] = "bid"
		case "book.title":
			arr[i] = "title"
		case "book.authors":
			arr[i] = "authors"
		case "book.publisher_id":
			arr[i] = "bpid"
		case "book.copies":
			arr[i] = "copies"
		case "customer.id":
			arr[i] = "cid"
		case "customer.name":
			arr[i] = "cname"
		case "customer.rank":
			arr[i] = "rank"
		case "orders.customer_id":
			arr[i] = "ocid"
		case "orders.book_id":
			arr[i] = "obid"
		case "orders.quantity":
			arr[i] = "quantity"
		}
	}
	for _, str := range arr {
		strout += str + " "
	}
	return strout
}

// ResetColsForProjection reset cols to get a unique colname
func ResetColsForProjection(strin string) (strout string) {
	strout = ""
	f := func(c rune) bool {
		if c == ' ' || c == ',' {
			return true
		}
		return false
	}
	arr := strings.FieldsFunc(strin, f)
	for i, str := range arr {
		switch str {
		case "publisher.id":
			arr[i] = "pid"
		case "publisher.name":
			arr[i] = "pname"
		case "publisher.nation":
			arr[i] = "nation"
		case "book.id":
			arr[i] = "bid"
		case "book.title":
			arr[i] = "title"
		case "book.authors":
			arr[i] = "authors"
		case "book.publisher_id":
			arr[i] = "bpid"
		case "book.copies":
			arr[i] = "copies"
		case "customer.id":
			arr[i] = "cid"
		case "customer.name":
			arr[i] = "cname"
		case "customer.rank":
			arr[i] = "rank"
		case "orders.customer_id":
			arr[i] = "ocid"
		case "orders.book_id":
			arr[i] = "obid"
		case "orders.quantity":
			arr[i] = "quantity"
		}
	}
	length := len(arr)
	for i, str := range arr {
		strout += str
		if i < length-1 {
			strout += ","
		}

	}
	return strout
}

// AddTableNode add table node
func (logicalPlanTree *PlanTree) AddTableNode(newNode PlanTreeNode) {
	if logicalPlanTree.NodeNum == 0 {
		root := logicalPlanTree.findEmptyNode()
		newNode.Nodeid = root
		logicalPlanTree.Nodes[root] = newNode
		logicalPlanTree.NodeNum++
		logicalPlanTree.Root = root
	} else {
		pos := logicalPlanTree.findEmptyNode()
		newNode.Nodeid = pos
		logicalPlanTree.Nodes[pos] = newNode
		logicalPlanTree.NodeNum++

		newroot := logicalPlanTree.findEmptyNode()
		logicalPlanTree.Nodes[newroot] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
		logicalPlanTree.NodeNum++

		root := logicalPlanTree.Root
		logicalPlanTree.Nodes[newroot].Nodeid = newroot
		logicalPlanTree.Nodes[newroot].Left = root
		logicalPlanTree.Nodes[newroot].Right = pos
		logicalPlanTree.Nodes[pos].Parent = newroot
		logicalPlanTree.Nodes[root].Parent = newroot
		logicalPlanTree.Root = newroot
	}
}

// AddSelectionNode add selection node
func (logicalPlanTree *PlanTree) AddSelectionNode(newNode PlanTreeNode) {
	newroot := logicalPlanTree.findEmptyNode()
	newNode.Nodeid = newroot
	root := logicalPlanTree.Root
	newNode.Left = root
	logicalPlanTree.Nodes[newroot] = newNode
	logicalPlanTree.NodeNum++
	logicalPlanTree.Nodes[root].Parent = newroot
	logicalPlanTree.Root = newroot
}

// AddProjectionNode add projection node
func (logicalPlanTree *PlanTree) AddProjectionNode(newNode PlanTreeNode) {
	newroot := logicalPlanTree.findEmptyNode()
	newNode.Nodeid = newroot
	root := logicalPlanTree.Root
	newNode.Left = root
	logicalPlanTree.Nodes[newroot] = newNode
	logicalPlanTree.NodeNum++
	logicalPlanTree.Nodes[root].Parent = newroot
	logicalPlanTree.Root = newroot
}

func (logicalPlanTree *PlanTree) buildBalanceTree() {
	orders := CreateTableNode("orders")
	customer := CreateTableNode("customer")
	publisher := CreateTableNode("publisher")
	book := CreateTableNode("book")

	opos := logicalPlanTree.findEmptyNode()
	orders.Nodeid = opos
	logicalPlanTree.Nodes[opos] = orders
	logicalPlanTree.NodeNum++

	cpos := logicalPlanTree.findEmptyNode()
	customer.Nodeid = cpos
	logicalPlanTree.Nodes[cpos] = customer
	logicalPlanTree.NodeNum++

	ocjoin := logicalPlanTree.findEmptyNode()
	logicalPlanTree.Nodes[ocjoin] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
	logicalPlanTree.Nodes[ocjoin].Nodeid = ocjoin
	logicalPlanTree.NodeNum++

	logicalPlanTree.Nodes[ocjoin].Left = opos
	logicalPlanTree.Nodes[ocjoin].Right = cpos
	logicalPlanTree.Nodes[opos].Parent = ocjoin
	logicalPlanTree.Nodes[cpos].Parent = ocjoin

	bpos := logicalPlanTree.findEmptyNode()
	book.Nodeid = bpos
	logicalPlanTree.Nodes[bpos] = book
	logicalPlanTree.NodeNum++

	ppos := logicalPlanTree.findEmptyNode()
	publisher.Nodeid = ppos
	logicalPlanTree.Nodes[ppos] = publisher
	logicalPlanTree.NodeNum++

	bpjoin := logicalPlanTree.findEmptyNode()
	logicalPlanTree.Nodes[bpjoin] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
	logicalPlanTree.Nodes[bpjoin].Nodeid = bpjoin
	logicalPlanTree.NodeNum++

	logicalPlanTree.Nodes[bpjoin].Left = bpos
	logicalPlanTree.Nodes[bpjoin].Right = ppos
	logicalPlanTree.Nodes[bpos].Parent = bpjoin
	logicalPlanTree.Nodes[ppos].Parent = bpjoin

	root := logicalPlanTree.findEmptyNode()
	logicalPlanTree.Nodes[root] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
	logicalPlanTree.Nodes[root].Nodeid = root
	logicalPlanTree.NodeNum++

	logicalPlanTree.Nodes[root].Left = ocjoin
	logicalPlanTree.Nodes[root].Right = bpjoin
	logicalPlanTree.Nodes[ocjoin].Parent = root
	logicalPlanTree.Nodes[bpjoin].Parent = root
	logicalPlanTree.Root = root
}

func (logicalPlanTree *PlanTree) buildSelect(sel *sqlparser.Select) {
	if sel.From == nil {
		fmt.Println("cannot build plan tree without From")
		os.Exit(1)
	}
	if len(sel.From) == 4 {
		// println("handle 4 tables!!!!")
		logicalPlanTree.buildBalanceTree()
	} else {
		for _, table := range sel.From {
			tableName := sqlparser.String(table)
			logicalPlanTree.AddTableNode(CreateTableNode(tableName))
		}
	}

	if sel.Where != nil {
		whereString := sqlparser.String(sel.Where.Expr)
		// fmt.Println("where string:", whereString)
		if len(sel.From) == 1 { // single table, add table name before columns
			// tmpString := strings.TrimSuffix(whereString, "where")
			tmpString := whereString
			whereString = ""
			conditions := strings.Split(tmpString, "and")
			for _, cond := range conditions {
				cond = strings.ReplaceAll(cond, " ", "")
				f := func(c rune) bool {
					return (c == ' ' || c == '=' || c == '<' || c == '>')
				}
				f1 := func(c rune) bool {
					return !(c == ' ' || c == '=' || c == '<' || c == '>')
				}

				operands := strings.FieldsFunc(cond, f)
				if len(operands) != 2 {
					continue
				}
				for i, oprd := range operands {
					if !CheckValue(oprd) {
						tableName := sqlparser.String(sel.From[0])
						if !strings.HasPrefix(oprd, tableName) {
							operands[i] = tableName + "." + operands[i]
						}
					}
				}
				op := strings.FieldsFunc(cond, f1)
				whereString += operands[0] + " " + op[0] + " " + operands[1] + " and "
			}
			whereString = strings.TrimSuffix(whereString, " and ")
		}
		// fmt.Println(whereString)
		// os.Exit(0)
		// whereString = ResetColsForWhere(whereString)
		// fmt.Println("where string:", whereString)
		logicalPlanTree.AddSelectionNode(CreateSelectionNode(logicalPlanTree.GetTmpTableName(), whereString))
	}

	if sel.SelectExprs == nil {
		fmt.Println("cannot build plan tree without select")
		os.Exit(1)
	}

	projectionString := sqlparser.String(sel.SelectExprs)
	// fmt.Println("projection string = ", projectionString)
	// projectionString = ResetColsForProjection(projectionString)
	// fmt.Println("projection string = ", projectionString)
	logicalPlanTree.AddProjectionNode(CreateProjectionNode(logicalPlanTree.GetTmpTableName(), projectionString))
	// logicalPlanTree.Root = root
	// return logicalPlanTree
}

func Parse(sql string, txnID int64) *PlanTree {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// add rollback..
		fmt.Printf("parse error: %v\n", err.Error())
		return nil
	}

	planTree := new(PlanTree)
	planTree.InitialPlanTree(txnID)

	switch stmt.(type) {
	case *sqlparser.Select:
		planTree.buildSelect(stmt.(*sqlparser.Select))

		// case *sqlparser.Update:
		// 	// return handleUpdate(stmt.(*sqlparser.Update))
		// case *sqlparser.Insert:
		// 	// return handleInsert(stmt.(*sqlparser.Insert))
		// 	buildInsert(stmt.(*sqlparser.Insert))
		// case *sqlparser.Delete:
		// 	// return handleDelete(stmt.(*sqlparser.Delete))
	}

	// ips, ports, siteNames, err := storage.FetchSites("Publisher")
	// if err != nil {
	// 	return nil
	// }

	// // union node
	// node1 := new(UnionOperator)
	// node1.ip = "10.77.50.211"
	// node1.siteName = "main"

	// // union node
	// node2 := new(UnionOperator)
	// node2.ip = "10.77.50.211"
	// node2.siteName = "main"
	// node1.left = node2
	// planTree.nodeNum = 1

	// // union node
	// node3 := new(UnionOperator)
	// node3.ip = "10.77.50.211"
	// node2.siteName = "main"
	// node1.right = node3

	// // scan node
	// node4 := new(ScanOperator)
	// node4.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node4.ip = ips[0]
	// node4.port = ports[0]
	// node4.tableName = "Publisher"
	// node4.siteName = siteNames[0]
	// node4.left = nil
	// node4.right = nil

	// // scan node
	// node5 := new(ScanOperator)
	// node5.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node5.ip = ips[1]
	// node5.port = ports[1]
	// node5.tableName = "Publisher"
	// node5.siteName = siteNames[1]
	// node5.left = nil
	// node5.right = nil

	// // scan node
	// node6 := new(ScanOperator)
	// node6.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node6.ip = ips[2]
	// node6.port = ports[2]
	// node6.tableName = "Publisher"
	// node6.siteName = siteNames[2]
	// node6.left = nil
	// node6.right = nil

	// // scan node
	// node7 := new(ScanOperator)
	// node7.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node7.ip = ips[3]
	// node7.port = ports[3]
	// node7.tableName = "Publisher"
	// node7.siteName = siteNames[3]
	// node7.left = nil
	// node7.right = nil

	// node2.left = node4
	// node2.right = node5
	// node3.left = node6
	// node3.right = node7

	// planTree.root = node1

	return planTree
}
