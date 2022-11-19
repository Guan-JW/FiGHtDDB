package parser

import (
	"strconv"

	"github.com/FiGHtDDB/storage"
)

type PlanTreeNode interface {
	LeftChild()		PlanTreeNode
	RightChild() 	PlanTreeNode
}

type SelectOperator struct {
	PlanTreeNode
	left PlanTreeNode
	right PlanTreeNode
	tableName string
	ip string
	port int
	db *storage.Db
	// some other things, such as column and condition
}

func (node *SelectOperator) LeftChild() PlanTreeNode {
	return node.left
}

func (node *SelectOperator) RightChild() PlanTreeNode {
	return node.right
}

func (node *SelectOperator) TableName() string {
	return node.tableName
}

func (node *SelectOperator) Site() string {
	return node.ip + strconv.Itoa(node.port)
}

func (node *SelectOperator) Ip() string {
	return node.ip
}

func (node *SelectOperator) Db() *storage.Db {
	return node.db
}

type PlanTree struct {
	nodeNum int
	root 	PlanTreeNode
}

func (tree *PlanTree) Root() PlanTreeNode {
	return tree.root
}