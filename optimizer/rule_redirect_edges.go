package optimizer

import (
	"strings"

	"github.com/FiGHtDDB/parser"
)

func redirect(pt *parser.PlanTree, NodeID int64, ParentID int64) {
	node := &pt.Nodes[NodeID]
	// if node.Parent == ParentID {
	// 	return
	// }
	// redirect edge
	if node.Parent != ParentID {
		ID := node.Parent
		cntID := NodeID
		for {
			if pt.Nodes[ID].NodeType == 2 || pt.Nodes[ID].NodeType == 3 {
				cntID = ID
				ID = pt.Nodes[ID].Parent
			} else {
				break
			}
		}

		parentNode := &pt.Nodes[ParentID]
		if parentNode.Left == NodeID {
			parentNode.Left = cntID
		} else {
			parentNode.Right = cntID
		}

		newTableName := pt.Nodes[cntID].TmpTable
		oldTableName := node.TmpTable

		if newTableName != oldTableName { // update table name of parent node
			// fmt.Println(parentNode)
			if parentNode.ExecStmtCols != "" {
				parentNode.ExecStmtCols = strings.ReplaceAll(parentNode.ExecStmtCols, oldTableName+".", newTableName+".")
			}
			if parentNode.ExecStmtWhere != "" {
				parentNode.ExecStmtWhere = strings.ReplaceAll(parentNode.ExecStmtWhere, oldTableName+".", newTableName+".")
			}
			// fmt.Println(parentNode)
			// os.Exit(0)
		}
	}

	if node.Left >= 0 {
		leftNode := pt.Nodes[node.Left]
		if leftNode.NodeType == 1 && leftNode.Left != -1 {
			newTableName := leftNode.TmpTable
			oldTableName := pt.Nodes[leftNode.Left].TmpTable
			if node.ExecStmtCols != "" {
				// fmt.Println("node.ExecStmtCols = ", node.ExecStmtCols)
				node.ExecStmtCols = strings.ReplaceAll(node.ExecStmtCols, oldTableName+".", newTableName+".")
				// fmt.Println("node.ExecStmtCols (new) = ", node.ExecStmtCols)
			}
			if node.ExecStmtWhere != "" {
				// fmt.Println("node.ExecStmtWhere = ", node.ExecStmtWhere)
				node.ExecStmtWhere = strings.ReplaceAll(node.ExecStmtWhere, oldTableName+".", newTableName+".")
				// fmt.Println("node.ExecStmtWhere (new) = ", node.ExecStmtWhere)
			}
			// os.Exit(0)
		}
	}

	if node.Right >= 0 {
		rightNode := pt.Nodes[node.Right]
		if rightNode.NodeType == 1 && rightNode.Left != -1 {
			newTableName := rightNode.TmpTable
			oldTableName := pt.Nodes[rightNode.Left].TmpTable
			if node.ExecStmtCols != "" {
				// fmt.Println("node.ExecStmtCols = ", node.ExecStmtCols)
				node.ExecStmtCols = strings.ReplaceAll(node.ExecStmtCols, oldTableName+".", newTableName+".")
				// fmt.Println("node.ExecStmtCols (new) = ", node.ExecStmtCols)
			}
			if node.ExecStmtWhere != "" {
				// fmt.Println("node.ExecStmtWhere = ", node.ExecStmtWhere)
				node.ExecStmtWhere = strings.ReplaceAll(node.ExecStmtWhere, oldTableName+".", newTableName+".")
				// fmt.Println("node.ExecStmtWhere (new) = ", node.ExecStmtWhere)
			}
		}
	}

	switch node.NodeType {
	case 1: // table ndoe
	case 2, 3:
		redirect(pt, node.Left, NodeID)
	case 4, 5:
		redirect(pt, node.Left, NodeID)
		redirect(pt, node.Right, NodeID)
	}

}

func RedirectEdges(pt *parser.PlanTree) *parser.PlanTree {
	redirect(pt, pt.Root, int64(-1))
	return pt
}
