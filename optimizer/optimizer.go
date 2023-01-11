package optimizer

import (
	"github.com/FiGHtDDB/parser"
)

func Optimize(pt *parser.PlanTree) *parser.PlanTree {
	loopmax := 1
	// var newtree planner.PlanTree
	// newtree = oldtree
	for i := 0; i < loopmax; i++ {
		pt = GetRelCols(pt)
		// // newtree = JointConditionPushDown(newtree)
		pt = SelectionPushDown(pt)
		pt = GetRelCols(pt)
		pt = PruneColumns(pt)
		pt = FilterMerge(pt)
		// pt = RootFilterRename(pt)
		pt = RedirectEdges(pt)

		// newtree = TransmissionMinimization(newtree)
		// // newtree = GetRelCols(newtree)
		// pt.Print()
		// os.Exit(0)
	}

	for i, node := range pt.Nodes {
		if node.Nodeid == -1 {
			continue
			// physicalPlanTree.Nodes[i].Nodeid = 0 //               !!!!!!!!
		} else if node.NodeType == 1 && !node.TransferFlag && node.Left == -1 {
			pt.Nodes[i].Status = 2
		} else if node.NodeType == -2 {
			pt.Nodes[i].Status = 2
		} else {
			pt.Nodes[i].Status = 0
		}
	}
	return pt
}
