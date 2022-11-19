package parser

import (
	"strconv"

	"github.com/FiGHtDDB/storage"
)

type ScanOperator struct {
	PlanTreeNode
	left PlanTreeNode
	right PlanTreeNode
	tableName string
	siteName  string
	ip string
	port int
	db *storage.Db
	// some other things, such as column and condition
}

func (node *ScanOperator) LeftChild() PlanTreeNode {
	return node.left
}

func (node *ScanOperator) RightChild() PlanTreeNode {
	return node.right
}

func (node *ScanOperator) TableName() string {
	return node.tableName
}

func (node *ScanOperator) Site() string {
	return node.ip + strconv.Itoa(node.port)
}

func (node *ScanOperator) Ip() string {
	return node.ip
}

func (node *ScanOperator) Db() *storage.Db {
	return node.db
}

func (node *ScanOperator) SiteName() string {
	return node.siteName
}