package parser

import (
	"github.com/FiGHtDDB/storage"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func Parse(sql string) *PlanTree {
	planTree := new(PlanTree)
	ips, ports, siteNames, err := storage.FetchSites("Publisher")
	if err != nil {
		return nil
	}

	// union node
	node1 := new(UnionOperator)
	node1.ip = "10.77.50.211"
	node1.siteName = "main"

	// union node
	node2 := new(UnionOperator)
	node2.ip = "10.77.50.211"
	node2.siteName = "main"
	node1.left = node2
	planTree.nodeNum = 1

	// union node
	node3 := new(UnionOperator)
	node3.ip = "10.77.50.211"
	node2.siteName = "main"
	node1.right = node3

	// scan node
	node4 := new(ScanOperator)
	node4.db = storage.NewDb("postgres", "postgres", "postgres", 5700, "disable")
	node4.ip = ips[0]
	node4.port = ports[0]
	node4.tableName = "Publisher"
	node4.siteName = siteNames[0]
	node4.left = nil
	node4.right = nil

	// scan node
	node5 := new(ScanOperator)
	node5.db = storage.NewDb("postgres", "postgres", "postgres", 5700, "disable")
	node5.ip = ips[1]
	node5.port = ports[1]
	node5.tableName = "Publisher"
	node5.siteName = siteNames[1]
	node5.left = nil
	node5.right = nil

	// scan node
	node6 := new(ScanOperator)
	node6.db = storage.NewDb("postgres", "postgres", "postgres", 5700, "disable")
	node6.ip = ips[2]
	node6.port = ports[2]
	node6.tableName = "Publisher"
	node6.siteName = siteNames[2]
	node6.left = nil
	node6.right = nil

	// scan node
	node7 := new(ScanOperator)
	node7.db = storage.NewDb("postgres", "postgres", "postgres", 5701, "disable")
	node7.ip = ips[3]
	node7.port = ports[3]
	node7.tableName = "Publisher"
	node7.siteName = siteNames[3]
	node7.left = nil
	node7.right = nil

	node2.left = node4
	node2.right = node5
	node3.left = node6
	node3.right = node7

	planTree.root = node1

	return planTree
}