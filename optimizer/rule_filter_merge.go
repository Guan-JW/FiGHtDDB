package optimizer

import (
	"fmt"
	"strings"

	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/storage"
)

func GetFragConditions(tableName string, locate string) []string {
	// fmt.Println(tableName)
	var rst []string

	Tmeta, err := storage.GetTableMeta(tableName)
	if err != nil {
		fmt.Println(err)
		return rst
	}
	// fmt.Println(Tmeta)

	// rst := ""
	for _, schema := range Tmeta.FragSchema {
		if schema.SiteName != locate {
			continue
		}

		if len(schema.Conditions) == 0 { // vertical
			// fmt.Println("vertical")
			for _, col := range schema.Cols {
				rst = append(rst, tableName+"."+col)
				// rst += tableName + "." + col + ","
			}
			// rst = strings.TrimSuffix(rst, ",")
			return rst
		} else { // horizontal
			// fmt.Println("horizontal")
			// TODO: deal with situations where cond.Value is an attribute
			for _, cond := range schema.Conditions {
				rst = append(rst, tableName+"."+cond.Col+" "+cond.Comp+" "+cond.Value)
				// rst += cond.Col + " " + cond.Comp + " " + cond.Value + " and "
			}
			// rst = strings.TrimSuffix(rst, " and ")
			return rst
		}
	}
	return rst
}

// prune repeated conditions and concate different conditons
func UpdateFilterConditions(filterConditions string, fragCondList []string) string {
	// fmt.Println("filterConditions = ", filterConditions)
	// fmt.Println("fragCond = ", fragCondList)
	strout := ""

	// fragCondList := strings.Split(fragCond, " and ")
	filterCondList := strings.Split(strings.TrimPrefix(filterConditions, "where  "), " and ")

	// fmt.Println("filter cond list : ", filterCondList, "; fragCondlist: ", fragCondList)
	// inefficient implementation
	for _, filterCond := range filterCondList {
		contain := false
		cond1 := strings.ReplaceAll(filterCond, " ", "") // delete all spaces
		for _, fragCond := range fragCondList {
			cond2 := strings.ReplaceAll(fragCond, " ", "")
			if cond1 == cond2 {
				contain = true
				break
			}
		}
		if !contain {
			strout += filterCond + " and "
		}
	}
	strout = strings.TrimSuffix(strout, " and ")
	// fmt.Println("After Updating: ", strout)
	return strout
}

// nodeid represents the child of the last selection node
// firstNodeid represents the first selection node
func addMergeWhereNode(pt *parser.PlanTree, newNode parser.PlanTreeNode, firstNodeid int64, lastNodeid int64, nodeid int64) {
	// if pt.Nodes[nodeid].NodeType == 4 && pt.Nodes[nodeid].Joint_type == 0 { // x join
	if pt.Nodes[nodeid].NodeType == 4 { // x join
		// pt.Nodes[nodeid].Joint_type = 1 // 此时where子句为a=b（？），转变成等值连接
		pt.Nodes[nodeid].Where = newNode.Where
	} else {
		newNodeid := findEmptyNode(pt)
		// fmt.Println("newNodeid=", newNodeid)
		newNode.Nodeid = newNodeid
		newNode.Parent = pt.Nodes[firstNodeid].Parent
		newNode.Left = nodeid
		newNode.Locate = pt.Nodes[nodeid].Locate
		newNode.TransferFlag = pt.Nodes[nodeid].TransferFlag
		newNode.Dest = pt.Nodes[nodeid].Dest
		// Rel_cols and Cols equal to the last selection node
		newNode.Rel_cols = pt.Nodes[lastNodeid].Rel_cols
		newNode.Cols = pt.Nodes[lastNodeid].Cols
		// fmt.Println("newNode.where = ", newNode.Where)

		if pt.Nodes[nodeid].TransferFlag {
			pt.Nodes[nodeid].TransferFlag = false
			pt.Nodes[nodeid].Dest = ""
		}
		switch getChildType(pt, firstNodeid) {
		case "Left":
			pt.Nodes[pt.Nodes[firstNodeid].Parent].Left = newNodeid
		case "Right":
			pt.Nodes[pt.Nodes[firstNodeid].Parent].Right = newNodeid
		case "err":
			println("Error: childType Error")
		}

		pt.Nodes[nodeid].Parent = newNodeid
		pt.Nodes[newNodeid] = newNode //向数组中加入节点
		// fmt.Println(pt.Nodes[newNodeid].Where)
		pt.NodeNum++

		// delete old selection nodes
		for nodeID := firstNodeid; ; {
			nextID := pt.Nodes[nodeID].Left
			pt.Nodes[nodeID] = parser.InitialPlanTreeNode()

			if nodeID == lastNodeid {
				break
			}
			nodeID = nextID
		}
	}
}

func merge_filters(pt *parser.PlanTree, beginNode int64) {
	node := pt.Nodes[beginNode]
	switch node.NodeType {
	case 2: //select
		oldWhere := node.Where
		filterConditions := node.Where
		for filter := &pt.Nodes[node.Left]; ; filter = &pt.Nodes[filter.Left] {

			if filter.NodeType != 2 { // finish of consecutive selection nodes
				// filterID := filter.Nodeid
				newWhere := ""
				// get fragment conditions
				fragCond := GetFragConditions(filter.TmpTable, filter.Locate)
				// if fragCond == "" {
				if len(fragCond) == 0 {
					newWhere = filterConditions
				} else { // delete repeated(redundant) conditions
					newWhere = UpdateFilterConditions(filterConditions, fragCond)
				}
				if newWhere != "" && !strings.HasPrefix(newWhere, "where") {
					newWhere = "where  " + newWhere
				}
				// fmt.Println("oldWhere=", oldWhere, "; newWhere=", newWhere)
				if oldWhere != newWhere { // update select node
					if newWhere == "" {
						// update edges
						parentID := node.Parent
						if parentID != -1 {
							switch getChildType(pt, node.Nodeid) {
							case "Left":
								pt.Nodes[parentID].Left = filter.Nodeid
							case "Right":
								pt.Nodes[parentID].Right = filter.Nodeid
							case "err":
								println("Error: childType Error")
							}
						}

						// delete old selection nodes
						for nodeID := node.Nodeid; ; {
							nextID := pt.Nodes[nodeID].Left
							pt.Nodes[nodeID] = parser.InitialPlanTreeNode()
							if nodeID == filter.Parent {
								break
							}
							nodeID = nextID
						}
						filter.Parent = parentID

					} else {
						// fmt.Println("Adding new Where node...")
						addMergeWhereNode(pt, parser.CreateSelectionNode(pt.GetTmpTableName(), newWhere), node.Nodeid, pt.Nodes[filter.Parent].Nodeid, filter.Nodeid)
					}
				}

				f := func(c rune) bool {
					return (c == ' ' || c == ',')
				}
				f1 := func(c rune) bool {
					return (c == ' ' || c == '=' || c == '<' || c == '>')
				}
				f2 := func(c rune) bool {
					return !(c == '=' || c == '<' || c == '>')
				}
				// fmt.Println("parent id = ", filter.Parent)
				// fmt.Println("Parent.Cols=", pt.Nodes[filter.Parent].Cols, "; Parent.Where=", pt.Nodes[filter.Parent].Where)
				if pt.Nodes[filter.Parent].Cols != "" {
					columns := strings.FieldsFunc(pt.Nodes[filter.Parent].Cols, f)
					ChildTableName := pt.Nodes[pt.Nodes[filter.Parent].Left].TmpTable // get child node's TmpTable name
					for i, col := range columns {                                     // replace table name with TmpTable
						tableCol := strings.Split(col, ".")
						if len(tableCol) == 1 {
							columns[i] = ChildTableName + "." + tableCol[0]
						} else if len(tableCol) == 2 {
							columns[i] = ChildTableName + "." + tableCol[1]
						}
						// else  // shouldn't get here
					}
					pt.Nodes[filter.Parent].ExecStmtCols = strings.Join(columns, ",")
					// fmt.Println("ExecStmtCols=", pt.Nodes[filter.Parent].ExecStmtCols)
				}

				if pt.Nodes[filter.Parent].Where != "" {
					ChildTmpTable := pt.Nodes[pt.Nodes[filter.Parent].Left].TmpTable
					conditions := strings.Split(strings.TrimPrefix(pt.Nodes[filter.Parent].Where, "where"), "and")
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
					pt.Nodes[filter.Parent].ExecStmtWhere = "where " + strings.Join(conditions, " and ")
					// fmt.Println("ExecStmtWhere=", pt.Nodes[filter.Parent].ExecStmtWhere)
					// fmt.Println("conditions = ", conditions)
				}

				merge_filters(pt, filter.Nodeid)
				break
			}

			filterConditions += "and " + strings.TrimPrefix(filter.Where, "where")
		}
		// os.Exit(0)
	case 3: //projection
		merge_filters(pt, node.Left)
	case 4, 5: //join and union
		merge_filters(pt, node.Left)
		merge_filters(pt, node.Right)
	}
}

func FilterMerge(pt *parser.PlanTree) *parser.PlanTree {
	// fmt.Println("!!!! Column Pruning !!!!")
	merge_filters(pt, pt.Root)

	// fmt.Println("Root.Cols: ", pt.Nodes[pt.Root].Cols)
	// fmt.Println("Root.Left.Cols: ", pt.Nodes[pt.Nodes[pt.Root].Left].Cols)

	// remove useless projection node on the top
	RootID := pt.Root
	ChildID := pt.Nodes[RootID].Left
	RootCols := pt.Nodes[RootID].Cols
	ChildCols := pt.Nodes[ChildID].Cols
	if (RootCols == "*" && ChildCols == "") || (RootCols == ChildCols) {
		// remove root
		pt.Root = ChildID
		pt.Nodes[ChildID].Parent = -1
		pt.Nodes[RootID] = parser.InitialPlanTreeNode()
	}
	// pt.Print()
	return pt
}
