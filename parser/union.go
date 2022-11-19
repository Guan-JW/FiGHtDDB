package parser

type UnionOperator struct {
	siteName string
	ip 		 string
	port     int
	left 	 PlanTreeNode
	right	 PlanTreeNode 
}

func (node *UnionOperator) LeftChild() PlanTreeNode {
	return node.left
}

func (node *UnionOperator) RightChild() PlanTreeNode {
	return node.right
}

func (node *UnionOperator) SiteName() string {
	return node.siteName
}