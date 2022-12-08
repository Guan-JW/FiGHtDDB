package parser

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/FiGHtDDB/storage"
)

type void struct{}

// ShowPlanTree print tree
func (planTree *PlanTree) ShowPlanTree() {
	fmt.Printf("NodeNum is %d\n", planTree.NodeNum)
	fmt.Printf("root is %d\n", planTree.Root)
	for _, node := range planTree.Nodes {
		if node.Nodeid != -1 {
			fmt.Println(node)
		}
	}
}

func splitOperands(c rune) bool {
	if c == ' ' || c == '=' || c == '<' || c == '>' {
		return true
	}
	return false
}

func splitOperators(c rune) bool {
	if c == '=' || c == '<' || c == '>' {
		return false
	}
	return true
}

// check if value is not an attribute name
func CheckValue(value string) bool {
	if strings.HasPrefix(value, "'") { // begin with '
		return true
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil { // is number
		return true
	}
	return false
}

// retrieve conditions that are related to TableName, and are not join conditions
func SplitHorizontalCond(condition string, TableName string) ([]storage.Condition, map[string]void) {
	var Conditions []storage.Condition
	set := make(map[string]void)

	conds := strings.Split(condition, "and")
	colMap := make(map[string]string)
	// fmt.Println(conds, len(conds))

	// get useful column names from join conditions
	// TODO: to support column rename, e.g. book.id bid
	for _, cond := range conds {
		// fmt.Println(cond)
		operands := strings.FieldsFunc(cond, splitOperands) // get operands
		if len(operands) < 2 {
			continue
		}
		if !CheckValue(operands[1]) { // join condition
			left_operand := strings.Split(operands[0], ".")
			right_operand := strings.Split(operands[1], ".")
			if left_operand[0] == TableName {
				colMap[operands[1]] = left_operand[1]
			} else if right_operand[0] == TableName {
				colMap[operands[0]] = right_operand[1]
				var member void
				set[right_operand[1]] = member
			}
		}
	}
	// fmt.Println(colMap)
	// os.Exit(0)

	for _, cond := range conds {
		operands := strings.FieldsFunc(cond, splitOperands) // get operands
		if len(operands) < 2 {
			continue
		}
		if !CheckValue(operands[1]) {
			continue
		}

		left_operand := strings.Split(operands[0], ".") // get left operand [table col]
		if left_operand[0] != TableName {
			// left operand belongs to another table
			// check if operands[0] is inside map
			col, ok := colMap[operands[0]]
			if ok {
				left_operand[1] = col // replace with the column name of TableName
			} else {
				continue
			}
		} else {
			var member void
			set[left_operand[1]] = member
		}

		op := strings.FieldsFunc(cond, splitOperators) // get operator
		// fmt.Println(operands, op)
		var TmpCond storage.Condition
		TmpCond.Col = left_operand[1]
		TmpCond.Type = ""
		TmpCond.Comp = op[0]
		TmpCond.Value = operands[1]
		Conditions = append(Conditions, TmpCond)
	}
	// fmt.Println("Horizontal conditions = ", Conditions)
	return Conditions, set
}

// retrieve columns of TableName
func SplitVerticalCond(condition string, TableName string) []string {
	f := func(c rune) bool {
		if c == ',' || c == ' ' {
			return true
		}
		return false
	}

	if condition == "" {
		return []string{""}
	}

	var vconds []string
	conds := strings.FieldsFunc(condition, f)
	for _, cond := range conds {
		tableAttrs := strings.Split(cond, ".")
		// fmt.Println(tableAttrs[0])
		if tableAttrs[0] == TableName { // check table
			vconds = append(vconds, tableAttrs[1])
		}
	}
	// fmt.Println("Vertical conditions = ", vconds)
	return vconds
}

// check if some items in FragConds conflict with HorConds
func CheckHorizontalConflict(FragConds *[]storage.Condition, HorConds *[]storage.Condition) bool {
	var conflict bool = false // no conflict
	for _, hc := range *HorConds {
		for _, fc := range *FragConds {
			if hc.Col != fc.Col {
				continue
			}
			// fmt.Println("hc=", hc)
			// fmt.Println("fc=", fc)
			// fmt.Println(hc.Comp, fc.Comp)
			switch hc.Comp {
			case "=":
				hvalue := hc.Value
				switch fc.Comp {
				case "=":
					conflict = (hvalue != fc.Value)
				case ">":
					conflict = (hvalue <= fc.Value)
				case "<":
					conflict = (hvalue >= fc.Value)
				case ">=":
					conflict = (hvalue < fc.Value)
				case "<=":
					conflict = (hvalue > fc.Value)
				}
				if conflict {
					return true
				}
			case ">":
				hvalue := hc.Value
				switch fc.Comp {
				case "=":
					conflict = (hvalue >= fc.Value)
				case ">", ">=":
					// do nothing
				case "<", "<=":
					conflict = (hvalue >= fc.Value)
				}
				if conflict {
					return true
				}
			case ">=":
				hvalue := hc.Value
				switch fc.Comp {
				case "=":
					conflict = (hvalue > fc.Value)
				case ">", ">=":
					// do nothing
				case "<", "<=":
					conflict = (hvalue > fc.Value)
				}
				if conflict {
					return true
				}
			case "<":
				hvalue := hc.Value
				switch fc.Comp {
				case "=":
					conflict = (hvalue <= fc.Value)
				case ">", ">=":
					conflict = (hvalue <= fc.Value)
				case "<", "<=":
					// do nothing
				}
				if conflict {
					return true
				}
			case "<=":
				hvalue := hc.Value
				switch fc.Comp {
				case "=":
					conflict = (hvalue < fc.Value)
				case ">", ">=":
					conflict = (hvalue < fc.Value)
				case "<", "<=":
					// do nothing
				}
				if conflict {
					return true
				}
			}
		}
	}

	return conflict
}

// check if col is used in only one fragment
// primary key is used in all fragments
func CheckIsPrimaryKey(Tmeta *storage.TableMeta, col string) bool {
	counter := 0
	for _, schema := range Tmeta.FragSchema {
		for _, sCol := range schema.Cols {
			if sCol == col {
				counter++
			}
		}
	}
	return counter == len(Tmeta.FragSchema)
}

// check if all columns in VerCols are contained in FragCols
// check if all columns in FragCols are used in VerCols
func CheckVerticalConflict(Tmeta *storage.TableMeta, FragCols *[]string, VerCols *[]string, ColSet *map[string]void) bool {
	conflict := true
	for _, fc := range *FragCols {
		// conflict := true // not all columns inside the fragment are used
		if CheckIsPrimaryKey(Tmeta, fc) { // skip primary key
			continue
		}
		for _, vc := range *VerCols {
			// fmt.Println("fc=", fc, "; vc=", vc)
			if fc == vc { // used
				conflict = false
				break
			}
		}
		if conflict { // fc is not used in selectClause, check whereClause
			// fmt.Println(fc, *ColSet)
			if _, ok := (*ColSet)[fc]; ok { // used
				conflict = false
			}
		}
		// fmt.Println(conflict)
		if !conflict {
			break
		}
	}
	return conflict
}

func FragmentFilter(TableName string, selectClause string, whereClause string) *storage.TableMeta {
	// fmt.Println("TableName is : ", TableName)
	// fmt.Println("WhereClause == ", whereClause, "; SelectClause == ", selectClause)
	HorConds, ColSet := SplitHorizontalCond(whereClause, TableName) // Condition
	// fmt.Println("HorConds == ", HorConds)
	// fmt.Println("ColSet == ", ColSet)
	VerCols := SplitVerticalCond(selectClause, TableName) // string
	// fmt.Println("VerCols == ", VerCols)

	wholeMeta, err := storage.GetTableMeta(TableName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	PartMeta := new(storage.TableMeta)
	PartMeta.TableName = TableName
	for _, schema := range wholeMeta.FragSchema {
		conflict := false
		if len(schema.Conditions) == 0 && selectClause != "*" { // vertical
			// fmt.Println("Cols = ", schema.Cols)
			conflict = CheckVerticalConflict(wholeMeta, &schema.Cols, &VerCols, &ColSet)
		} else { // horizontal
			conflict = CheckHorizontalConflict(&schema.Conditions, &HorConds)
		}
		if !conflict {
			PartMeta.FragSchema = append(PartMeta.FragSchema, schema)
		}
		// fmt.Println("is conflict = ", conflict)
	}
	PartMeta.FragNum = len(PartMeta.FragSchema)
	// fmt.Println("wholeMeta:", wholeMeta)
	// fmt.Println("PartMeta:", PartMeta)
	// os.Exit(0)
	return PartMeta
}

func createTableNode(tableName string, siteName string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 1
	node.TmpTable = tableName
	node.Locate = siteName
	return node
}

func (physicalPlanTree *PlanTree) addTableNode(newNode PlanTreeNode, root int64, fragType int64) int64 {
	if root == -1 {
		root = physicalPlanTree.findEmptyNode()
		newNode.Nodeid = root
		physicalPlanTree.Nodes[root] = newNode
		physicalPlanTree.NodeNum++
	} else {
		pos := physicalPlanTree.findEmptyNode()
		newNode.Nodeid = pos
		physicalPlanTree.Nodes[pos] = newNode
		physicalPlanTree.NodeNum++

		newroot := physicalPlanTree.findEmptyNode()
		if fragType == 0 { // horizontal
			physicalPlanTree.Nodes[newroot] = CreateUnionNode(physicalPlanTree.GetTmpTableName())
			physicalPlanTree.NodeNum++
		} else { // vertical
			physicalPlanTree.Nodes[newroot] = CreateJoinNode(physicalPlanTree.GetTmpTableName(), 2)
			physicalPlanTree.NodeNum++
		}

		physicalPlanTree.Nodes[newroot].Nodeid = newroot
		physicalPlanTree.Nodes[newroot].Left = root
		physicalPlanTree.Nodes[newroot].Right = pos
		physicalPlanTree.Nodes[pos].Parent = newroot
		physicalPlanTree.Nodes[root].Parent = newroot
		root = newroot

	}
	return root
}

func (physicalPlanTree *PlanTree) getChildType(id int64) string {
	if physicalPlanTree.Nodes[physicalPlanTree.Nodes[id].Parent].Left == id {
		return "Left"
	} else if physicalPlanTree.Nodes[physicalPlanTree.Nodes[id].Parent].Right == id {
		return "Right"
	}

	return "err"
}

func (physicalPlanTree *PlanTree) replace(old int64, new int64) {
	physicalPlanTree.Nodes[new].Parent = physicalPlanTree.Nodes[old].Parent
	switch physicalPlanTree.getChildType(old) {
	case "Left":
		physicalPlanTree.Nodes[physicalPlanTree.Nodes[old].Parent].Left = new
	case "Right":
		physicalPlanTree.Nodes[physicalPlanTree.Nodes[old].Parent].Right = new
	default:
		fmt.Println("parent and child relationship is wrong")
	}
	physicalPlanTree.Nodes[old] = InitialPlanTreeNode()
	// physicalPlanTree.NodeNum--

}

func (physicalPlanTree *PlanTree) splitTableNode(tableNode PlanTreeNode, selectClause string, whereClause string) {
	Tmeta := FragmentFilter(tableNode.TmpTable, selectClause, whereClause)
	// fmt.Println("Tmeta.FragNum = ", Tmeta.FragNum)
	if Tmeta.FragNum <= 0 { // no partition
		return
	}
	var root int64 = -1
	for i := int(0); i < Tmeta.FragNum; i++ {
		if len(Tmeta.FragSchema[i].Conditions) == 0 { // vertical
			root = physicalPlanTree.addTableNode(createTableNode(tableNode.TmpTable, Tmeta.FragSchema[i].SiteName), root, 1)
		} else {
			root = physicalPlanTree.addTableNode(createTableNode(tableNode.TmpTable, Tmeta.FragSchema[i].SiteName), root, 0)
		}
	}
	// ShowPlanTree(physicalPlanTree)
	physicalPlanTree.replace(tableNode.Nodeid, root)
}

func min(a string, b string) (min string, info string) {
	// utilities.LoadAllConfig()
	// clientSite := utilities.GetMe().NodeId
	clientSite := "main"
	// fmt.Println("a:", a, "b:", b)
	if a == b {
		min = a
		info = "Equal"
	} else if a == clientSite {
		min = a
		info = "Left"
	} else if b == clientSite {
		min = b
		info = "Right"
	} else if a < b {
		min = a
		info = "Left"
	} else if a > b {
		min = b
		info = "Right"
	}
	return min, info
}

func (physicalPlanTree *PlanTree) getLocate(i int64) (locate string) {
	if physicalPlanTree.Nodes[i].Locate != "" {
		locate = physicalPlanTree.Nodes[i].Locate
	} else if physicalPlanTree.Nodes[i].Left != -1 && physicalPlanTree.Nodes[i].Right != -1 {
		//如果存在client站点，则优先client，否则取两个孩子中Locate小的那个
		var minchildinfo string
		locate, minchildinfo = min(physicalPlanTree.getLocate(physicalPlanTree.Nodes[i].Left), physicalPlanTree.getLocate(physicalPlanTree.Nodes[i].Right))
		switch minchildinfo {
		case "Left":
			physicalPlanTree.Nodes[physicalPlanTree.Nodes[i].Left].TransferFlag = false
			physicalPlanTree.Nodes[physicalPlanTree.Nodes[i].Right].TransferFlag = true
			physicalPlanTree.Nodes[physicalPlanTree.Nodes[i].Right].Dest = locate
		case "Right":
			physicalPlanTree.Nodes[physicalPlanTree.Nodes[i].Right].TransferFlag = false
			physicalPlanTree.Nodes[physicalPlanTree.Nodes[i].Left].TransferFlag = true
			physicalPlanTree.Nodes[physicalPlanTree.Nodes[i].Left].Dest = locate
		case "Equal":

		}
	} else if physicalPlanTree.Nodes[i].Left == -1 && physicalPlanTree.Nodes[i].Right != -1 {
		locate = physicalPlanTree.getLocate(physicalPlanTree.Nodes[i].Right)

	} else if physicalPlanTree.Nodes[i].Left != -1 && physicalPlanTree.Nodes[i].Right == -1 {
		locate = physicalPlanTree.getLocate(physicalPlanTree.Nodes[i].Left)
	} else {
		fmt.Println("error when getlocate, there is a node without child but don't have locate")
		os.Exit(1)
	}
	return locate
}

// Analyze can transfer a logicalPlanTree to a physicalPlanTree
func (physicalPlanTree *PlanTree) Analyze() {
	// get from and where clauses
	root := physicalPlanTree.Nodes[physicalPlanTree.Root]
	selectClause := root.Cols
	whereClause := ""
	if physicalPlanTree.Nodes[root.Left].NodeType == 2 {
		whereClause = physicalPlanTree.Nodes[root.Left].Where
	}
	for _, node := range physicalPlanTree.Nodes {
		if node.NodeType == 1 {
			physicalPlanTree.splitTableNode(node, selectClause, whereClause)
		}
	}
	// physicalPlanTree.ShowPlanTree()
	// os.Exit(0)
	// fmt.Println("node num = ", physicalPlanTree.NodeNum)
	for i, node := range physicalPlanTree.Nodes {
		if node.Nodeid == -1 {
			// fmt.Println("Continue..")
			continue
		} else if node.Locate == "" {
			physicalPlanTree.Nodes[i].Locate = physicalPlanTree.getLocate(int64(i))
			// fmt.Println("locate=", physicalPlanTree.Nodes[i].Locate)
		} else {
			// fmt.Println("locate=", physicalPlanTree.Nodes[i].Locate)
		}
	}
	for i, node := range physicalPlanTree.Nodes {
		if node.Nodeid == -1 {
			continue
			// physicalPlanTree.Nodes[i].Nodeid = 0 //               !!!!!!!!
		} else if node.NodeType == 1 && !node.TransferFlag {
			physicalPlanTree.Nodes[i].Status = 1
		} else {
			physicalPlanTree.Nodes[i].Status = 0
		}
	}
}
