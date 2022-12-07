package optimizer

import (
	"strings"

	"github.com/FiGHtDDB/parser"
)

type void struct{}

// a rude way for column checking, because columns may contain the same name
// e.g. cid and ocid
func CheckExistInParent(col string, parentCols string) bool {
	if parentCols == "" || col == "" {
		return false
	}
	if parentCols == col {
		return true
	}

	if strings.HasPrefix(parentCols, col+",") { // begin with col
		return true
	} else if strings.Contains(parentCols, ","+col+",") { // contain col
		return true
	} else if strings.HasSuffix(parentCols, ","+col) { // end with col
		return true
	}
	return false
}

// return true if cols equals to rel_cols,
// which means no need to prune for the rest of the tree
// It is assumed that the user will not select the same attribute repeatedly
// TODO: add column name checking
func CheckSelectAll(Rel_cols string, Cols string) bool {
	cols1 := strings.Split(Rel_cols, ",")
	cols2 := strings.Split(Cols, ",")
	return len(cols1) == len(cols2)
}

// get attributes used in the where clause
func GetWhereCols(whereClause string, parentCols string) map[string]void {
	strout := make(map[string]void)
	f := func(c rune) bool {
		if c == ' ' || c == '=' || c == '<' || c == '>' {
			return true
		}
		return false
	}
	// pCols := strings.Split(parentCols, ",")
	// whereClause = strings.ReplaceAll(whereClause, " ", "")
	where := strings.TrimPrefix(whereClause, "where")
	operands := strings.FieldsFunc(where, f)
	// fmt.Println(operands)
	// only handle simple where clause
	if len(operands) == 2 {
		if !CheckExistInParent(operands[0], parentCols) {
			// strout = append(strout, operands[0])
			var member void
			strout[operands[0]] = member
			// strout += operands[0]
		}
		if !parser.CheckValue(operands[1]) &&
			!CheckExistInParent(operands[1], parentCols) {
			// if strout != "" {
			// 	strout += ", "
			// }
			// strout += operands[1]
			var member void
			strout[operands[1]] = member
			// strout = append(strout, operands[1])
		}
	}
	// fmt.Println(strout)
	return strout
}

func AddProjectionNodeAboveTable(pt *parser.PlanTree, newNode parser.PlanTreeNode, nodeid int64) {
	if pt.Nodes[nodeid].NodeType != 1 {
		return
	}
	newNodeid := findEmptyNode(pt)
	newNode.Nodeid = newNodeid
	newNode.Parent = pt.Nodes[nodeid].Parent
	newNode.Left = nodeid
	newNode.Locate = pt.Nodes[nodeid].Locate
	newNode.TransferFlag = pt.Nodes[nodeid].TransferFlag
	newNode.Dest = pt.Nodes[nodeid].Dest
	newNode.Rel_cols = pt.Nodes[nodeid].Rel_cols
	// newNode.Cols = pt.Nodes[nodeid].Cols

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
	pt.Nodes[newNodeid] = newNode
	pt.NodeNum++

}

func prune_columns(pt *parser.PlanTree, beginNode int64, parentCols string) {
	// fmt.Println(" *********** BEGIN NODE : ", beginNode)

	f := func(c rune) bool {
		return (c == ' ' || c == ',')
	}
	f1 := func(c rune) bool {
		return (c == ' ' || c == '=' || c == '<' || c == '>')
	}
	f2 := func(c rune) bool {
		return !(c == '=' || c == '<' || c == '>')
	}

	node := &pt.Nodes[beginNode]
	switch node.NodeType {
	case 1:
		// table
		// fmt.Println("!!!!!! Table Node !!!!!!")
		// node.Cols = parentCols
		// if CheckSelectAll(node.Rel_cols, node.Cols) {
		// 	node.Cols = "*"
		// }
		if pt.Nodes[node.Parent].NodeType == 5 && !CheckSelectAll(node.Rel_cols, parentCols) { // add projection node if parent node is Union, because union node doesn't contain filter
			AddProjectionNodeAboveTable(pt, parser.CreateProjectionNode(pt.GetTmpTableName(), parentCols), beginNode)
			prune_columns(pt, pt.Nodes[node.Parent].Nodeid, pt.Nodes[node.Parent].Cols)
		}

	case 2:
		// select
		// fmt.Println("!!!!!! Selection Node !!!!!!")
		// pt.Nodes[beginNode].Cols, _ = GetUsedAttr(beginNode)
		whereCols := GetWhereCols(node.Where, parentCols)
		node.Cols = parentCols
		used := node.Cols
		for wc, _ := range whereCols {
			if !strings.Contains(used, wc) {
				used += ", " + wc
			}
		}
		// fmt.Println("node.Cols = ", node.Cols)

		// fmt.Println("node.Rel_cols = ", node.Rel_cols)
		if CheckSelectAll(node.Rel_cols, node.Cols) {
			node.Cols = ""
			node.ExecStmtCols = ""
		}
		// fmt.Println("node.Cols = ", node.Cols, "node.Where=", node.Where)
		prune_columns(pt, pt.Nodes[node.Left].Nodeid, used)

	case 3:
		// projection
		// GetUsedAttr(beginNode)
		// fmt.Println("!!!!!! Projection Node !!!!!!")
		f := func(c rune) bool {
			if c == ' ' || c == ',' {
				return true
			}
			return false
		}
		if node.Cols != "*" && node.Cols != "" { // means need to select something, return
			if parentCols != "" {
				node.Cols = parentCols
				usedCols := strings.FieldsFunc(node.Cols, f)
				for _, col := range usedCols {
					if !CheckExistInParent(col, parentCols) {
						node.Cols += "," + node.Cols
					}
				}
			}
			// fmt.Println(node.Cols)

			if CheckSelectAll(node.Rel_cols, node.Cols) {
				node.Cols = ""
				node.ExecStmtCols = ""
			}
			prune_columns(pt, pt.Nodes[node.Left].Nodeid, node.Cols)

		} else {
			node.Cols = ""
		}

	case 4:
		// join
		// TODO: be more efficient
		// fmt.Println("!!!!!! Join Node !!!!!!")
		whereCols := GetWhereCols(node.Where, parentCols)
		// fmt.Println("WHERE COLS: ", whereCols)
		node.Cols = parentCols
		// fmt.Println("node.Cols = ", node.Cols)

		used := node.Cols
		for wc, _ := range whereCols {
			if !strings.Contains(used, wc) {
				used += ", " + wc
			}
		}
		// fmt.Println("used = ", used)
		usedCols := strings.FieldsFunc(used, f)
		// get children's Rel_cols
		leftRelCols := strings.FieldsFunc(pt.Nodes[node.Left].Rel_cols, f)
		rightRelCols := strings.FieldsFunc(pt.Nodes[node.Right].Rel_cols, f)
		// get children's TmpTable name
		leftTmpTable := pt.Nodes[node.Left].TmpTable
		rightTmpTable := pt.Nodes[node.Right].TmpTable

		subsetLeft := ""
		subsetRight := ""
		// fmt.Println("usedCols = ", usedCols)
		// fmt.Println("leftRelCols = ", leftRelCols)
		// fmt.Println("rightRelCols = ", rightRelCols)

		node.ExecStmtCols = node.Cols
		node.ExecStmtWhere = node.Where
		// inefficient for big tables
		for _, col := range usedCols {
			// fmt.Println("col=", col)
			// col = strings.ReplaceAll(col, " ", "")
			for _, lcol := range leftRelCols {
				// fmt.Println("lcol=", lcol)

				tableCol := strings.Split(lcol, ".")
				if col == lcol || col == tableCol[1] {
					// fmt.Println("left equal!")
					subsetLeft += lcol + ","

					// check if lcol is inside of whereClause
					if _, ok := whereCols[lcol]; ok {
						// replace table name with leftTmpTable
						if len(tableCol) == 2 {
							node.ExecStmtWhere = strings.Replace(node.ExecStmtWhere, lcol, leftTmpTable+"."+tableCol[1], -1)
						} else if len(tableCol) == 1 {
							node.ExecStmtWhere = strings.Replace(node.ExecStmtWhere, lcol, leftTmpTable+"."+tableCol[0], -1)
						}
					}

					// replace table name with leftTmpTable -- fromClause
					if len(tableCol) == 2 {
						node.ExecStmtCols = strings.Replace(node.ExecStmtCols, lcol, leftTmpTable+"."+tableCol[1], -1)
					} else {
						node.ExecStmtCols = strings.Replace(node.ExecStmtCols, lcol, leftTmpTable+"."+tableCol[0], -1)
					}

					break
				}
			}
			for _, rcol := range rightRelCols {
				// fmt.Println("rcol=", rcol)
				tableCol := strings.Split(rcol, ".")
				if col == rcol || col == tableCol[1] { // col contains tableName or not
					// fmt.Println("right equal!")
					subsetRight += rcol + ","

					// check if rcol is inside of whereClause
					if _, ok := whereCols[rcol]; ok {
						// fmt.Println("node.ExecStmtWhere = ", node.ExecStmtWhere)
						// fmt.Println("tableCol = ", tableCol)
						// fmt.Println("rcol = ", rcol)
						if len(tableCol) == 2 {
							node.ExecStmtWhere = strings.ReplaceAll(node.ExecStmtWhere, rcol, rightTmpTable+"."+tableCol[1])
							// fmt.Println("node.ExecStmtWhere = ", node.ExecStmtWhere)
							// if col == "customer.id" {
							// 	os.Exit(0)
							// }

						} else if len(tableCol) == 1 {
							node.ExecStmtWhere = strings.ReplaceAll(node.ExecStmtWhere, rcol, rightTmpTable+"."+tableCol[0])
						}
					}

					// replace table name with leftTmpTable -- fromClause
					if len(tableCol) == 2 {
						node.ExecStmtCols = strings.ReplaceAll(node.ExecStmtCols, rcol, rightTmpTable+"."+tableCol[1])
					} else {
						node.ExecStmtCols = strings.ReplaceAll(node.ExecStmtCols, rcol, rightTmpTable+"."+tableCol[0])
					}
					break
				}
			}
		}
		subsetLeft = strings.TrimSuffix(subsetLeft, ",")
		subsetRight = strings.TrimSuffix(subsetRight, ",")

		// fmt.Println("subsetLeft: ", subsetLeft)
		// fmt.Println("subsetRight: ", subsetRight)
		if CheckSelectAll(node.Rel_cols, node.Cols) {
			node.Cols = ""
			node.ExecStmtCols = ""
		}
		prune_columns(pt, pt.Nodes[node.Left].Nodeid, subsetLeft)
		prune_columns(pt, pt.Nodes[node.Right].Nodeid, subsetRight)

	case 5:
		// union
		// no where clause
		// fmt.Println("!!!!!! Union Node !!!!!!")
		node.Cols = parentCols
		// fmt.Println("node.Cols : ", node.Cols)
		usedCols := strings.FieldsFunc(node.Cols, f)
		leftRelCols := strings.FieldsFunc(pt.Nodes[node.Left].Rel_cols, f)
		rightRelCols := strings.FieldsFunc(pt.Nodes[node.Right].Rel_cols, f)
		// get children's TmpTable name
		// leftTmpTable := pt.Nodes[node.Left].TmpTable
		// rightTmpTable := pt.Nodes[node.Right].TmpTable
		// fmt.Println("usedCols = ", usedCols)
		// fmt.Println("leftRelCols = ", leftRelCols)
		// fmt.Println("rightRelCols = ", rightRelCols)

		subsetLeft := ""
		subsetRight := ""

		// replace tablename with TmpTable
		// inefficient for tables with a large number of columns
		node.ExecStmtCols = node.Cols
		for _, col := range usedCols {
			for _, lcol := range leftRelCols {
				tableCol := strings.Split(lcol, ".")
				// fmt.Println("col=", col, "; lcol=", lcol)
				if col == lcol || col == tableCol[1] { // col contains tableName
					subsetLeft += lcol + ","
					// tableCol := strings.Split(col, ".")
					// // replace table name with leftTmpTable -- fromClause
					// if len(tableCol) == 2 {
					// 	node.ExecStmtCols = strings.Replace(node.ExecStmtCols, col, leftTmpTable+"."+tableCol[1], -1)
					// } else {
					// 	node.ExecStmtCols = strings.Replace(node.ExecStmtCols, col, leftTmpTable+"."+tableCol[0], -1)
					// }
					break
				}
			}
			for _, rcol := range rightRelCols {
				tableCol := strings.Split(rcol, ".")
				// fmt.Println("col=", col, "; rcol=", rcol)
				if col == rcol || col == tableCol[1] {
					subsetRight += rcol + ","
					// tableCol := strings.Split(col, ".")
					// // replace table name with leftTmpTable -- fromClause
					// if len(tableCol) == 2 {
					// 	node.ExecStmtCols = strings.Replace(node.ExecStmtCols, col, rightTmpTable+"."+tableCol[1], -1)
					// } else {
					// 	node.ExecStmtCols = strings.Replace(node.ExecStmtCols, col, rightTmpTable+"."+tableCol[0], -1)
					// }
					break
				}
			}
		}
		subsetLeft = strings.TrimSuffix(subsetLeft, ",")
		subsetRight = strings.TrimSuffix(subsetRight, ",")

		if CheckSelectAll(node.Rel_cols, node.Cols) {
			node.Cols = ""
			node.ExecStmtCols = ""
		}
		prune_columns(pt, pt.Nodes[node.Left].Nodeid, subsetLeft)
		prune_columns(pt, pt.Nodes[node.Right].Nodeid, subsetRight)

	default:
		// fmt.Println("!!!! Default !!!!")
		node.Cols = "" // *
	}

	// fmt.Println("node.Cols = ", node.Cols, "; node.Where=", node.Where)

	// replace tablename with TmpTable
	// only process projection node here
	// processing of selection nodes are moved to rule_filter_merge.go
	if node.NodeType == 3 {
		if node.Cols != "" {
			columns := strings.FieldsFunc(node.Cols, f)
			ChildTableName := pt.Nodes[node.Left].TmpTable // get child node's TmpTable name
			for i, col := range columns {                  // replace table name with TmpTable
				tableCol := strings.Split(col, ".")
				if len(tableCol) == 1 {
					columns[i] = ChildTableName + "." + tableCol[0]
				} else if len(tableCol) == 2 {
					columns[i] = ChildTableName + "." + tableCol[1]
				}
				// else  // shouldn't get here
			}
			node.ExecStmtCols = strings.Join(columns, ",")
			// fmt.Println("ExecStmtCols=", node.ExecStmtCols)
		}

		if node.Where != "" {
			ChildTmpTable := pt.Nodes[node.Left].TmpTable
			conditions := strings.Split(strings.TrimPrefix(node.Where, "where"), "and")
			// fmt.Println("conditions = ", conditions)
			for i, cond := range conditions {
				operands := strings.FieldsFunc(cond, f1)
				op := strings.FieldsFunc(cond, f2)
				for j, oprd := range operands {
					if !parser.CheckValue(oprd) { // oprd is an attribute
						// fmt.Println("Operand = ", oprd, "; op = ", op)
						tableCol := strings.Split(oprd, ".")
						if len(tableCol) == 2 {
							operands[j] = ChildTmpTable + "." + tableCol[1]
						} else {
							// do something
						}
					}
					// fmt.Println(i, j)
				}
				if len(operands) == 2 && len(op) == 1 {
					conditions[i] = operands[0] + " " + op[0] + " " + operands[1]
				} else {
					// do something
				}
			}
			node.ExecStmtWhere = "where " + strings.Join(conditions, " and ")
			// fmt.Println("ExecStmtWhere=", node.ExecStmtWhere)
			// fmt.Println("conditions = ", conditions)
		}
	}
}

// add alias to the root node
func RootFilterRename(pt *parser.PlanTree) *parser.PlanTree {
	f := func(c rune) bool {
		return (c == ',' || c == ' ')
	}
	cols := strings.FieldsFunc(pt.Nodes[pt.Root].ExecStmtCols, f)
	for i, col := range cols {
		tableCol := strings.Split(col, ".")
		if len(tableCol) == 2 {
			cols[i] += " as " + tableCol[1] // alias
		}
		// else {
		// 	// do something
		// }
	}
	pt.Nodes[pt.Root].ExecStmtCols = strings.Join(cols, ", ")
	return pt
}

func PruneColumns(pt *parser.PlanTree) *parser.PlanTree {
	// fmt.Println("!!!! Column Pruning !!!!")
	prune_columns(pt, pt.Root, "")
	// pt.Print()
	return pt
}
