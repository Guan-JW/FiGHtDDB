package optimizer

import (
	"strings"

	"github.com/FiGHtDDB/parser"
)

func find2ChildNode(pt *parser.PlanTree, curNode int64) (pos int64) {
	for node := pt.Nodes[curNode]; ; node = pt.Nodes[node.Left] {
		if node.Left != -1 && node.Right != -1 {
			return node.Nodeid
		} else if node.Left == -1 && node.Right == -1 {
			return -1
		}

	}
}

func getChildType(pt *parser.PlanTree, childid int64) string {
	if pt.Nodes[pt.Nodes[childid].Parent].Left == childid {
		return "Left"
	} else if pt.Nodes[pt.Nodes[childid].Parent].Right == childid {
		return "Right"
	}

	return "err"
}

func findEmptyNode(pt *parser.PlanTree) int64 {
	for i, node := range pt.Nodes {
		if i != 0 && node.Nodeid == -1 {
			return int64(i)
		}
	}
	println("Error when creating node, no empty node left!")
	return -1
}

// 把节点加在输入节点的上方
func addWhereNodeOnTop(pt *parser.PlanTree, newNode parser.PlanTreeNode, nodeid int64) {
	// if pt.Nodes[nodeid].NodeType == 4 && pt.Nodes[nodeid].Joint_type == 0 { // x join

	if pt.Nodes[nodeid].NodeType == 4 {
		// fmt.Println("Here!!!! left =", pt.Nodes[nodeid].Left, "; right = ", pt.Nodes[nodeid].Right)
		// pt.Nodes[nodeid].Joint_type = 1 // 此时where子句为a=b（？），转变成等值连接
		pt.Nodes[nodeid].Where = newNode.Where
	} else {
		newNodeid := findEmptyNode(pt)
		newNode.Nodeid = newNodeid
		newNode.Parent = pt.Nodes[nodeid].Parent
		newNode.Left = nodeid
		newNode.Locate = pt.Nodes[nodeid].Locate
		newNode.TransferFlag = pt.Nodes[nodeid].TransferFlag
		newNode.Dest = pt.Nodes[nodeid].Dest

		if pt.Nodes[nodeid].TransferFlag {
			pt.Nodes[nodeid].TransferFlag = false
			pt.Nodes[nodeid].Dest = ""
		}
		switch getChildType(pt, nodeid) {
		case "Left":
			pt.Nodes[pt.Nodes[nodeid].Parent].Left = newNodeid
		case "Right":
			pt.Nodes[pt.Nodes[nodeid].Parent].Right = newNodeid
		case "err":
			println("Error: childType Error")
		}

		pt.Nodes[nodeid].Parent = newNodeid
		pt.Nodes[newNodeid] = newNode //向数组中加入节点
		pt.NodeNum++
		pt = GetRelCols(pt)
	}
}

func isCol(str string) bool {
	var cols = []string{"pid", "pname", "nation", "bid", "title", "authors", "bpid", "copies", "cid", "cname", "rank", "ocid", "obid", "quantity"}
	for _, col := range cols {
		if str == col {
			return true
		}
	}
	return false
}

func splitRelCols(subWhere string) []string {
	f := func(c rune) bool {
		if c == ' ' || c == '=' || c == '<' || c == '>' {
			return true
		}
		return false
	}
	subWhere = strings.TrimPrefix(subWhere, "where")
	// subWhere = strings.TrimPrefix(subWhere, " ")
	// fmt.Println("subWhere = ", subWhere)
	arr := strings.FieldsFunc(subWhere, f)
	// fmt.Println("arr = ", arr)
	ColCount := 0
	reCols := []string{}
	for _, str := range arr {
		if !parser.CheckValue(str) {
			ColCount++
			reCols = append(reCols, str)
		}
	}
	return reCols
}

func isInclude(strin string, arr []string) bool {
	for _, str := range arr {
		if strin == str {
			return true
		}
	}
	return false
}

// subwhere里包含的col是否在relcols中也全部都有
func checkCols(parentWhere string, childrelColsinComma string) bool {
	f := func(c rune) bool {
		if c == ' ' || c == ',' {
			return true
		}
		return false
	}
	// fmt.Println("parentWhere = ", parentWhere)
	// fmt.Println("childrelColsinComma = ", childrelColsinComma)
	Cols := splitRelCols(parentWhere)
	childRelCols := strings.FieldsFunc(childrelColsinComma, f)
	for _, col := range Cols {
		if !isInclude(col, childRelCols) {
			return false
		}
	}
	return true
}

func tryPushDown(pt *parser.PlanTree, subWhere string, beginNode int64, parentID int64) {
	if pt.Nodes[beginNode].Parent != parentID { // skip node with multiple edges (leave only one edge)
		return
	}

	pos := find2ChildNode(pt, beginNode)
	if pos == -1 { //若为-1则说明没有两个孩子的节点，只能加在curPos上，此时beginNode为selection类型
		addWhereNodeOnTop(pt, parser.CreateSelectionNode(pt.GetTmpTableName(), subWhere), beginNode)
	} else {
		flag1 := checkCols(subWhere, pt.Nodes[pt.Nodes[pos].Left].Rel_cols)
		flag2 := checkCols(subWhere, pt.Nodes[pt.Nodes[pos].Right].Rel_cols)
		if !flag1 && !flag2 { // 此时where子句为（ a=b 型等值？）连接
			// fmt.Println("left = ", pt.Nodes[pos].Left, "; right = ", pt.Nodes[pos].Right)
			addWhereNodeOnTop(pt, parser.CreateSelectionNode(pt.GetTmpTableName(), subWhere), pos)
		}
		// os.Exit(0)
		if flag1 {
			tryPushDown(pt, subWhere, pt.Nodes[pos].Left, pos)
		}
		if flag2 {
			tryPushDown(pt, subWhere, pt.Nodes[pos].Right, pos)
		}
	}
}

func deleteWhereNode(pt *parser.PlanTree, nodeid int64) {
	node := pt.Nodes[nodeid]
	pt.Nodes[node.Parent].Left = node.Left
	pt.Nodes[node.Left].Parent = node.Parent
	pt.Nodes[nodeid] = parser.InitialPlanTreeNode()
}

func SelectionPushDown(pt *parser.PlanTree) *parser.PlanTree {
	if pt.Root < 0 {
		return pt
	} else if pt.Nodes[pt.Root].NodeType >= 6 {
		return pt
	}
	for _, node := range pt.Nodes {
		if node.NodeType == 2 {
			//按照and分割where子句
			//方法：先按照空格分割，然后检测and来组合
			wheres := strings.Split(node.Where, "and")
			// wheres := strings.Split("book.copies > 100", "and")
			for _, subWhere := range wheres {
				subWhere = "where  " + subWhere
				// fmt.Println("subWhere = ", subWhere)
				// pt.Print()
				tryPushDown(pt, subWhere, node.Nodeid, node.Parent)
				// pt.Print()
				// return pt
			}
			deleteWhereNode(pt, node.Nodeid)
			break
		}
	}

	for i, node := range pt.Nodes {
		if node.NodeType == 1 && !node.TransferFlag {
			pt.Nodes[i].Status = 1
		} else {
			pt.Nodes[i].Status = 0
		}
	}

	return pt
}
