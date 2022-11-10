package parser


type PlanTreeNode struct {

}

type PlanTree struct {
	nodeNum int
	root 	*PlanTreeNode
}

func Parse(sql string) *PlanTree {
	query := queryCreate(sql)
	query.attrname = append(query.attrname, "xxx")

	planTree := new(PlanTree)
	planTree.nodeNum = 0
	planTree.root = nil
}