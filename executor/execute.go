package executor

import (
	"fmt"
	"log"

	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/storage"
)

var (
	ServerIp = ""
	ServerName = ""
)

// return type?
// consider we may project, union and join later
func executeNode(node parser.PlanTreeNode, resp *[]byte) {
	if node == nil {
		return
	}

	resp1 := make([]byte, 0)
	executeNode(node.LeftChild(), &resp1)
	resp2 := make([]byte, 0)
	executeNode(node.RightChild(), &resp2)

	// handle current node
	switch node := node.(type) {
	case *parser.ScanOperator:
		executeScanOperator(node, resp)
	case *parser.UnionOperator:
		executeUnionOperatpr(node, resp, &resp1, &resp2)
	default:
		log.Fatal("Unimpletemented node type")
	}
}

func executeUnionOperatpr(node *parser.UnionOperator, resp *[]byte, respLeftChild *[]byte, respRightChild *[]byte) {
	*resp = append(*resp, *respLeftChild...)
	*resp = append(*resp, *respRightChild...)
}

func executeScanOperator(node *parser.ScanOperator, resp *[]byte) {
	// judge if this operator executed by this site
	// if so, connect to pg and get result
	// else, send select str to correspoding node
	if ServerName == node.SiteName() {
		// fetch tuples from local database
		node.Db().FetchTuples(node.TableName(), resp)
	} else {
		// construct sqlStr according to Scan operator and send it to anoter server
		sqlStr := fmt.Sprintf("select * from %s;", node.TableName())
		storage.FetchRemoteTuples(sqlStr, node.Site(), resp)
	}
}

func Execute(tree *parser.PlanTree, resp *[]byte) int32 {
	executeNode(tree.Root(), resp)

	return 0
}