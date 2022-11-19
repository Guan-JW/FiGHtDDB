package parser

type PlanTreeNode interface {
	LeftChild()		PlanTreeNode
	RightChild() 	PlanTreeNode
	SiteName()		string
}

type PlanTree struct {
	nodeNum int
	root 	PlanTreeNode
}

func (tree *PlanTree) Root() PlanTreeNode {
	return tree.root
}