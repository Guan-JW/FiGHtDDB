package executor

import (
	"log"

	"github.com/FiGHtDDB/parser"
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
	executeNode(node.LeftChild(), resp)
	// handle result return from left child
	// ...
	executeNode(node.RightChild(), resp)
	// handle result return from right child
	// ...

	// handle current node
	switch node := node.(type) {
	case *parser.ScanOperator:
		executeScanOperator(node, resp)
	default:
		log.Fatal("Unimpletemented node type")
	}
}

func executeScanOperator(node *parser.ScanOperator, resp *[]byte) {
	// judge if this operator executed by this site
	// if so, connect to pg and get result
	// else, send select str to correspoding node
	if ServerName == node.SiteName() {
		// fetch tuples from local database
		node.Db().FetchTuples(node.TableName(), resp)
	} else {
		// TODO: send sqlStr to anoter server
		log.Fatal("Unimplemeted branch")
	}
}

func Execute(tree *parser.PlanTree, resp *[]byte) int32 {
	executeNode(tree.Root(), resp)

	return 0
}