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
		pt = RootFilterRename(pt)
		pt = RedirectEdges(pt)

		// newtree = TransmissionMinimization(newtree)
		// // newtree = GetRelCols(newtree)
		// pt.Print()
		// os.Exit(0)
	}
	return pt
}
