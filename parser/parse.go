package parser

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/FiGHtDDB/storage"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/xwb1989/sqlparser"
)

// CreateTableNode create table node
func CreateTableNode(tableName string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 1
	node.TmpTable = tableName
	return node
}

// CreateSelectionNode create selection nnode
func CreateSelectionNode(TmpTableName string, where string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 2
	node.TmpTable = TmpTableName
	node.Where = where
	return node
}

// CreateProjectionNode create projection node
func CreateProjectionNode(TmpTableName string, cols string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 3
	node.TmpTable = TmpTableName
	node.Cols = cols
	return node
}

// CreateJoinNode create join node
func CreateJoinNode(TmpTableName string, JointType int64) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 4
	node.TmpTable = TmpTableName
	// node.Joint_type = JointType
	return node
}

// CreateUnionNode create union node
func CreateUnionNode(TmpTableName string) PlanTreeNode {
	node := InitialPlanTreeNode()
	node.NodeType = 5
	node.TmpTable = TmpTableName
	return node
}

// ResetColsForWhere reset cols to get a unique colname
func ResetColsForWhere(strin string) (strout string) {
	strout = ""
	f := func(c rune) bool {
		if c == ' ' || c == ',' {
			return true
		}
		return false
	}
	arr := strings.FieldsFunc(strin, f)
	for i, str := range arr {
		switch str {
		case "publisher.id":
			arr[i] = "pid"
		case "publisher.name":
			arr[i] = "pname"
		case "publisher.nation":
			arr[i] = "nation"
		case "book.id":
			arr[i] = "bid"
		case "book.title":
			arr[i] = "title"
		case "book.authors":
			arr[i] = "authors"
		case "book.publisher_id":
			arr[i] = "bpid"
		case "book.copies":
			arr[i] = "copies"
		case "customer.id":
			arr[i] = "cid"
		case "customer.name":
			arr[i] = "cname"
		case "customer.rank":
			arr[i] = "rank"
		case "orders.customer_id":
			arr[i] = "ocid"
		case "orders.book_id":
			arr[i] = "obid"
		case "orders.quantity":
			arr[i] = "quantity"
		}
	}
	for _, str := range arr {
		strout += str + " "
	}
	return strout
}

// ResetColsForProjection reset cols to get a unique colname
func ResetColsForProjection(strin string) (strout string) {
	strout = ""
	f := func(c rune) bool {
		if c == ' ' || c == ',' {
			return true
		}
		return false
	}
	arr := strings.FieldsFunc(strin, f)
	for i, str := range arr {
		switch str {
		case "publisher.id":
			arr[i] = "pid"
		case "publisher.name":
			arr[i] = "pname"
		case "publisher.nation":
			arr[i] = "nation"
		case "book.id":
			arr[i] = "bid"
		case "book.title":
			arr[i] = "title"
		case "book.authors":
			arr[i] = "authors"
		case "book.publisher_id":
			arr[i] = "bpid"
		case "book.copies":
			arr[i] = "copies"
		case "customer.id":
			arr[i] = "cid"
		case "customer.name":
			arr[i] = "cname"
		case "customer.rank":
			arr[i] = "rank"
		case "orders.customer_id":
			arr[i] = "ocid"
		case "orders.book_id":
			arr[i] = "obid"
		case "orders.quantity":
			arr[i] = "quantity"
		}
	}
	length := len(arr)
	for i, str := range arr {
		strout += str
		if i < length-1 {
			strout += ","
		}

	}
	return strout
}

// add parent node on top of leftChild and RightChild, return parent id
func (pt *PlanTree) AddParentNode(newNode PlanTreeNode, leftChildId int64, rightChildId int64) int64 {
	id := pt.findEmptyNode()
	pt.Nodes[id] = newNode
	pt.Nodes[id].Nodeid = id
	pt.Nodes[id].Left = leftChildId
	pt.Nodes[id].Right = rightChildId
	pt.Nodes[leftChildId].Parent = id
	pt.Nodes[rightChildId].Parent = id
	pt.NodeNum++
	return id
}

// AddTableNode add table node
func (logicalPlanTree *PlanTree) AddTableNode(newNode PlanTreeNode) {
	if logicalPlanTree.NodeNum == 0 {
		root := logicalPlanTree.findEmptyNode()
		newNode.Nodeid = root
		logicalPlanTree.Nodes[root] = newNode
		logicalPlanTree.NodeNum++
		logicalPlanTree.Root = root
	} else {
		pos := logicalPlanTree.findEmptyNode()
		newNode.Nodeid = pos
		logicalPlanTree.Nodes[pos] = newNode
		logicalPlanTree.NodeNum++

		newroot := logicalPlanTree.findEmptyNode()
		logicalPlanTree.Nodes[newroot] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
		logicalPlanTree.NodeNum++

		root := logicalPlanTree.Root
		logicalPlanTree.Nodes[newroot].Nodeid = newroot
		logicalPlanTree.Nodes[newroot].Left = root
		logicalPlanTree.Nodes[newroot].Right = pos
		logicalPlanTree.Nodes[pos].Parent = newroot
		logicalPlanTree.Nodes[root].Parent = newroot
		logicalPlanTree.Root = newroot
	}
}

func (logicalPlanTree *PlanTree) addFragNode(newNode PlanTreeNode) int64 {
	pos := logicalPlanTree.findEmptyNode()
	newNode.Nodeid = pos
	logicalPlanTree.Nodes[pos] = newNode
	logicalPlanTree.NodeNum++
	return pos
}

// AddVerticalFragJoinNode add fragment nodes and join nodes for vertical fragments
func (logicalPlanTree *PlanTree) AddVerticalFragJoinNode(Tmeta *storage.TableMeta) int64 {
	id := logicalPlanTree.addFragNode(createTableNode(Tmeta.TableName, Tmeta.FragSchema[0].SiteName))
	for i := int(1); i < Tmeta.FragNum; i++ {
		id1 := logicalPlanTree.addFragNode(createTableNode(Tmeta.TableName, Tmeta.FragSchema[i].SiteName))
		newroot := logicalPlanTree.findEmptyNode()
		logicalPlanTree.Nodes[newroot] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 2)
		logicalPlanTree.Nodes[newroot].Nodeid = newroot
		logicalPlanTree.Nodes[newroot].Left = id
		logicalPlanTree.Nodes[newroot].Right = id1
		// locate, _ := min(logicalPlanTree.Nodes[id].Locate, logicalPlanTree.Nodes[id1].Locate)
		// logicalPlanTree.Nodes[newroot].Locate = locate
		logicalPlanTree.Nodes[id].Parent = newroot
		logicalPlanTree.Nodes[id1].Parent = newroot
		logicalPlanTree.NodeNum++
		id = newroot
	}
	return id
}

// AddFilterNode add selection or projection node
func (logicalPlanTree *PlanTree) AddFilternNode(newNode PlanTreeNode) {
	newroot := logicalPlanTree.findEmptyNode()
	newNode.Nodeid = newroot
	if logicalPlanTree.Root > 0 {
		root := logicalPlanTree.Root
		newNode.Left = root
		logicalPlanTree.Nodes[root].Parent = newroot
	}
	logicalPlanTree.Nodes[newroot] = newNode
	logicalPlanTree.Root = newroot
	logicalPlanTree.NodeNum++
}

func (logicalPlanTree *PlanTree) buildBalanceTree() {
	orders := CreateTableNode("orders")
	customer := CreateTableNode("customer")
	publisher := CreateTableNode("publisher")
	book := CreateTableNode("book")

	opos := logicalPlanTree.findEmptyNode()
	orders.Nodeid = opos
	logicalPlanTree.Nodes[opos] = orders
	logicalPlanTree.NodeNum++

	cpos := logicalPlanTree.findEmptyNode()
	customer.Nodeid = cpos
	logicalPlanTree.Nodes[cpos] = customer
	logicalPlanTree.NodeNum++

	ocjoin := logicalPlanTree.findEmptyNode()
	logicalPlanTree.Nodes[ocjoin] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
	logicalPlanTree.Nodes[ocjoin].Nodeid = ocjoin
	logicalPlanTree.NodeNum++

	logicalPlanTree.Nodes[ocjoin].Left = opos
	logicalPlanTree.Nodes[ocjoin].Right = cpos
	logicalPlanTree.Nodes[opos].Parent = ocjoin
	logicalPlanTree.Nodes[cpos].Parent = ocjoin

	bpos := logicalPlanTree.findEmptyNode()
	book.Nodeid = bpos
	logicalPlanTree.Nodes[bpos] = book
	logicalPlanTree.NodeNum++

	ppos := logicalPlanTree.findEmptyNode()
	publisher.Nodeid = ppos
	logicalPlanTree.Nodes[ppos] = publisher
	logicalPlanTree.NodeNum++

	bpjoin := logicalPlanTree.findEmptyNode()
	logicalPlanTree.Nodes[bpjoin] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
	logicalPlanTree.Nodes[bpjoin].Nodeid = bpjoin
	logicalPlanTree.NodeNum++

	logicalPlanTree.Nodes[bpjoin].Left = bpos
	logicalPlanTree.Nodes[bpjoin].Right = ppos
	logicalPlanTree.Nodes[bpos].Parent = bpjoin
	logicalPlanTree.Nodes[ppos].Parent = bpjoin

	root := logicalPlanTree.findEmptyNode()
	logicalPlanTree.Nodes[root] = CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0)
	logicalPlanTree.Nodes[root].Nodeid = root
	logicalPlanTree.NodeNum++

	logicalPlanTree.Nodes[root].Left = ocjoin
	logicalPlanTree.Nodes[root].Right = bpjoin
	logicalPlanTree.Nodes[ocjoin].Parent = root
	logicalPlanTree.Nodes[bpjoin].Parent = root
	logicalPlanTree.Root = root
}

func f(c rune) bool {
	return (c == ' ' || c == '=' || c == '<' || c == '>')
}
func f1(c rune) bool {
	return !(c == '=' || c == '<' || c == '>')
}

func CheckConflictPossibility(table1 string, table2 string, col1 string, col2 string, TmetaMap *map[string]*storage.TableMeta) bool {
	Tmeta1 := (*TmetaMap)[table1]
	if len(Tmeta1.FragSchema[0].Conditions) == 0 { // vertical
		return false
	}

	Tmeta2 := (*TmetaMap)[table2]
	if len(Tmeta2.FragSchema[0].Conditions) == 0 { // vertical
		return false
	}

	conflict1 := false
	conflict2 := false
	for _, cond := range Tmeta1.FragSchema[0].Conditions {
		if cond.Col == col1 {
			conflict1 = true
		}
	}
	for _, cond := range Tmeta2.FragSchema[0].Conditions {
		if cond.Col == col2 {
			conflict2 = true
		}
	}
	// fmt.Println("May be conflict: ", conflict1 && conflict2)
	// os.Exit(0)
	return conflict1 && conflict2
}

func ValueConflict(Cond1 *storage.Condition, Cond2 *storage.Condition) bool {
	var conflict bool = false
	switch Cond2.Comp {
	case "=":
		hvalue := Cond2.Value
		switch Cond1.Comp {
		case "=":
			conflict = (hvalue != Cond1.Value)
		case ">":
			conflict = (hvalue <= Cond1.Value)
		case "<":
			conflict = (hvalue >= Cond1.Value)
		case ">=":
			conflict = (hvalue < Cond1.Value)
		case "<=":
			conflict = (hvalue > Cond1.Value)
		}
		if conflict {
			return true
		}
	case ">":
		hvalue := Cond2.Value
		switch Cond1.Comp {
		case "=":
			conflict = (hvalue >= Cond1.Value)
		case ">", ">=":
			// do nothing
		case "<", "<=":
			conflict = (hvalue >= Cond1.Value)
		}
		if conflict {
			return true
		}
	case ">=":
		hvalue := Cond2.Value
		switch Cond1.Comp {
		case "=":
			conflict = (hvalue > Cond1.Value)
		case ">", ">=":
			// do nothing
		case "<", "<=":
			conflict = (hvalue > Cond1.Value)
		}
		if conflict {
			return true
		}
	case "<":
		hvalue := Cond2.Value
		switch Cond1.Comp {
		case "=":
			conflict = (hvalue <= Cond1.Value)
		case ">", ">=":
			conflict = (hvalue <= Cond1.Value)
		case "<", "<=":
			// do nothing
		}
		if conflict {
			return true
		}
	case "<=":
		hvalue := Cond2.Value
		switch Cond1.Comp {
		case "=":
			conflict = (hvalue < Cond1.Value)
		case ">", ">=":
			conflict = (hvalue < Cond1.Value)
		case "<", "<=":
			// do nothing
		}
		if conflict {
			return true
		}
	}
	return conflict
}

// Create sub-tree for one list
func (logicalPlanTree *PlanTree) CreateSubTree(FragIdMap *map[string]map[string]int64, patterns *[]string, clientSite string) int64 {
	root := int64(-1)
	// TODO: deal with more than 4 tables efficiently
	// build a balanced sub-tree
	// TODO: join reorder based on cost
	if len(*patterns) == 4 {
		// JoinLeftID := int64(-1)
		// JoinRightID := int64(-1)
		JoinIDs := []int64{0, 0}
		i := 0
		for {
			ptn := (*patterns)[i] // ptn = (publisher.main|publisher.segment2)

			leftID := int64(-1)
			if strings.HasPrefix(ptn, "(") {
				leftID = (*FragIdMap)[ptn][clientSite]
			} else {
				ts := strings.Split(ptn, ".") // [customer, main]
				leftID = (*FragIdMap)[ts[0]][ts[1]]
			}

			ptn = (*patterns)[i+1] // ptn = (book.main|book.segment1|book.segment2)
			rightID := int64(-1)
			if strings.HasPrefix(ptn, "(") {
				rightID = (*FragIdMap)[ptn][clientSite]
			} else {
				ts := strings.Split(ptn, ".") // [customer, main]
				rightID = (*FragIdMap)[ts[0]][ts[1]]
			}

			// create join node!!!!
			JoinIDs[int(i/2)] = logicalPlanTree.AddParentNode(CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0), leftID, rightID)

			i += 2
			if i >= len(*patterns) {
				break
			}
		}
		root = logicalPlanTree.AddParentNode(CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0), JoinIDs[0], JoinIDs[1])
	} else {
		leftID := int64(-1)
		for i, ptn := range *patterns {
			tmpID := int64(-1)
			if strings.HasPrefix(ptn, "(") {
				tmpID = (*FragIdMap)[ptn][clientSite]
			} else {
				ts := strings.Split(ptn, ".") // [customer, main]
				tmpID = (*FragIdMap)[ts[0]][ts[1]]
			}
			if i == 0 {
				leftID = tmpID
			} else {
				rightID := tmpID
				leftID = logicalPlanTree.AddParentNode(CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0), leftID, rightID)
			}
		}
		root = leftID
	}

	return root
}

// Create sub-Tree for one Fraglist
func (logicalPlanTree *PlanTree) FindAndCreateReuseNode(JoinFragLists *[]string, FragIdMap *map[string]map[string]int64, size int, clientSite string) int64 {
	root := int64(-1)
	for r, list := range *JoinFragLists { // list = (publisher.main|publisher.segment2)^(book.main|book.segment1|book.segment2)^orders.segment2^customer.main
		patterns := strings.Split(list, "^")
		for _, ptn := range patterns { // ptn = (publisher.main|publisher.segment2)
			if strings.HasPrefix(ptn, "(") {
				if _, ok := (*FragIdMap)[ptn]; !ok { // not exist
					// create union nodes, start with clientSite
					str := strings.TrimPrefix(ptn, "(")
					str = strings.TrimSuffix(str, ")")       // publisher.main|publisher.segment2
					tableSiteList := strings.Split(str, "|") // main site always comes first, already sorted

					listLength := len(tableSiteList)
					mask := make([]bool, listLength)
					leftID := int64(-1)
					useCount := 0
					i := 0
					for {
						if mask[i] {
							continue
						}
						tableSite := tableSiteList[i]
						ts := strings.Split(tableSite, ".") // [publisher, main]
						// step1: find clientSite
						if ts[1] == clientSite {
							leftID = (*FragIdMap)[ts[0]][ts[1]]
							mask[i] = true
							useCount++
							i++
							continue
						}
						// step2: create union node
						if !mask[i] && leftID >= 0 {
							rightID := (*FragIdMap)[ts[0]][ts[1]]
							// create union node

							// unionID := logicalPlanTree.findEmptyNode()
							// logicalPlanTree.Nodes[unionID] = CreateUnionNode(logicalPlanTree.GetTmpTableName())
							// logicalPlanTree.Nodes[unionID].Nodeid = unionID
							// logicalPlanTree.Nodes[unionID].Left = leftID
							// logicalPlanTree.Nodes[unionID].Right = rightID
							// logicalPlanTree.Nodes[leftID].Parent = unionID
							// logicalPlanTree.Nodes[rightID].Parent = unionID
							// logicalPlanTree.NodeNum++
							// leftID = unionID
							leftID = logicalPlanTree.AddParentNode(CreateUnionNode(logicalPlanTree.GetTmpTableName()), leftID, rightID)
							mask[i] = true
							useCount++
						}
						// all nodes are unioned
						if useCount == listLength {
							break
						}

						i++
						if i >= listLength {
							i = 0
						}
					}
					// step3: record ID in map
					(*FragIdMap)[ptn] = map[string]int64{clientSite: leftID}
				}

			}
		}

		// for tableName, Map := range *FragIdMap {
		// 	fmt.Println(tableName, ":")
		// 	for site, id := range Map {
		// 		fmt.Println("\t", site, ":", id)
		// 	}
		// }
		// step4: create sub-tree for current list
		if r == 0 {
			root = logicalPlanTree.CreateSubTree(FragIdMap, &patterns, clientSite)
		} else {
			newRoot := logicalPlanTree.CreateSubTree(FragIdMap, &patterns, clientSite)
			// step5: create union node on top of list pairs
			root = logicalPlanTree.AddParentNode(CreateUnionNode(logicalPlanTree.GetTmpTableName()), root, newRoot)
		}

		// os.Exit(0)
	}
	return root
}

func (logicalPlanTree *PlanTree) CollectPatterns(JoinTableLists *[][]string, JoinFragLists *[][]string, FragIdMap *map[string]map[string]int64) {

	// TODO: get clientSite from meta
	clientSite := "main"

	root := int64(-1)
	for i, _ := range *JoinFragLists {
		size := len((*JoinTableLists)[i])
		// fix tables except for the j-th one and find patterns
		for j := size - 1; j >= 0; j-- {
			patternMap := make(map[string]map[string]void) // use map for
			// fmt.Println("FragList = ", (*JoinFragLists)[i])
			for _, list := range (*JoinFragLists)[i] { // e.g. list = customer.main^orders.segment2^book.main^publisher.main
				tableSites := strings.Split(list, "^") // [customer.main, orders.segment2, book.main, publisher.main]
				fixedTableSites := ""
				for k := 0; k < size; k++ {
					if k == j {
						continue
					}
					fixedTableSites += tableSites[k] + "^"
				}
				fixedTableSites = strings.TrimSuffix(fixedTableSites, "^")
				// fmt.Println("fixedTableSites=", fixedTableSites)
				if _, ok := patternMap[fixedTableSites]; ok {
					if _, ok1 := patternMap[fixedTableSites][tableSites[j]]; !ok1 {
						var member void
						patternMap[fixedTableSites][tableSites[j]] = member
					}
				} else {
					var member void
					patternMap[fixedTableSites] = map[string]void{tableSites[j]: member}
				}
			}
			// fmt.Println("patternMap = ", patternMap)
			(*JoinFragLists)[i] = make([]string, 0)
			for key, val := range patternMap {
				createPattern := false
				pattern := "("
				tsList := make([]string, 0)
				if len(patternMap) > 1 {
					for ts := range val { // ts = publisher.main
						tsList = append(tsList, ts)
						if len(val) > 1 && strings.HasSuffix(ts, "."+clientSite) { // .main
							createPattern = true
						}
					}
				}
				if createPattern {
					sort.Strings(tsList)
					pattern += strings.Join(tsList, "|")
					pattern += ")"
					(*JoinFragLists)[i] = append((*JoinFragLists)[i], key+"^"+pattern)
				} else {
					for ts := range val {
						(*JoinFragLists)[i] = append((*JoinFragLists)[i], key+"^"+ts)
					}
				}
			}
			// fmt.Println("(*JoinFragLists)[", i, "] = ", (*JoinFragLists)[i])
		}
		if i == 0 {
			root = logicalPlanTree.FindAndCreateReuseNode(&(*JoinFragLists)[i], FragIdMap, size, clientSite)
		} else {
			newRoot := logicalPlanTree.FindAndCreateReuseNode(&(*JoinFragLists)[i], FragIdMap, size, clientSite)
			root = logicalPlanTree.AddParentNode(CreateJoinNode(logicalPlanTree.GetTmpTableName(), 0), root, newRoot)
		}
	}
	// fmt.Println(*JoinFragLists)
	logicalPlanTree.Root = root
}

func (logicalPlanTree *PlanTree) GetJoinUnionOrders(sel *sqlparser.Select, JoinConditions []string, projString string, whereString string) {

	TmetaMap := make(map[string]*storage.TableMeta) // tableName : TableMeta
	FragIdMap := make(map[string]map[string]int64)
	// FragIdMap := make(map[string]int64)             // customer.main: id

	for _, table := range sel.From {
		tableName := sqlparser.String(table)
		Tmeta := FragmentFilter(tableName, projString, whereString)
		TmetaMap[tableName] = Tmeta
		FragIdMap[tableName] = make(map[string]int64)
		if len(Tmeta.FragSchema[0].Conditions) == 0 { // vertical, join first
			id := logicalPlanTree.AddVerticalFragJoinNode(Tmeta)
			FragIdMap[tableName][logicalPlanTree.Nodes[id].Locate] = id // only save the join node
		} else { // horizontal
			for i := int(0); i < Tmeta.FragNum; i++ {
				id := logicalPlanTree.addFragNode(createTableNode(tableName, Tmeta.FragSchema[i].SiteName))
				FragIdMap[tableName][Tmeta.FragSchema[i].SiteName] = id
			}
		}
	}
	// for tableName, Tmeta := range TmetaMap {
	// 	fmt.Println(tableName)
	// 	fmt.Println(Tmeta)
	// }
	// for tableName, Map := range FragIdMap {
	// 	fmt.Println(tableName, ":")
	// 	for site, id := range Map {
	// 		fmt.Println("\t", site, ":", id)
	// 	}
	// }
	// os.Exit(0)

	var JoinTableLists [][]string // [[customer, orders, book, publisher]]
	var JoinFragLists [][]string  // [[[customer.main^orders.segment3, ...]]
	for _, cond := range JoinConditions {
		// e.g. 1. customer.id = orders.customer_id
		// e.g. 2. book.id = orders.book_id
		operands := strings.FieldsFunc(cond, f)      // get left and right operands, e.g. [customer.id, orders.customer_id]
		tableCol1 := strings.Split(operands[0], ".") // [customer, id]
		tableCol2 := strings.Split(operands[1], ".") // [orders, customer_id]
		table1 := tableCol1[0]                       // customer
		col1 := tableCol1[1]                         // id
		table2 := tableCol2[0]                       // orders
		col2 := tableCol2[1]                         // customer_id

		if len(JoinTableLists) == 0 {
			JoinTableLists = append(JoinTableLists, []string{table1, table2})     // [[customer, orders]]
			FragId1 := FragIdMap[table1]                                          // main: 1 (customer)
			FragId2 := FragIdMap[table2]                                          // segment2: 7; segment3: 8. (orders)
			if !CheckConflictPossibility(table1, table2, col1, col2, &TmetaMap) { // no conflict
				// case1: vertical
				// case2: at least one column is not used for splitting fragments
				for site1 := range FragId1 { // main (customer)
					tableSite1 := table1 + "." + site1 // customer.main
					for site2 := range FragId2 {       // segment2, segment3
						tableSite2 := table2 + "." + site2 // orders.segment2
						if len(JoinFragLists) == 0 {
							JoinFragLists = append(JoinFragLists, []string{tableSite1 + "^" + tableSite2}) // [[customer.main^orders.segmenet2]]
						} else {
							JoinFragLists[0] = append(JoinFragLists[0], tableSite1+"^"+tableSite2) // [[customer.main^orders.segmenet2, customer.main^orders.segment3]]
						}
					}
				}
			} else { // horizontal
				Tmeta1 := TmetaMap[table1]
				Tmeta2 := TmetaMap[table2]
				for _, schema1 := range Tmeta1.FragSchema {
					tableSite1 := table1 + "." + schema1.SiteName
					for _, schema2 := range Tmeta2.FragSchema {
						tableSite2 := table2 + "." + schema2.SiteName
						var conflict = false
						// fmt.Println(i, j)
						for _, cond1 := range schema1.Conditions {
							if cond1.Col != col1 {
								continue
							}
							for _, cond2 := range schema2.Conditions {
								if cond2.Col != col2 {
									continue
								}
								conflict = ValueConflict(&cond1, &cond2)
								if conflict {
									break
								}
							}
							if conflict {
								break
							}
						}
						if !conflict {
							if len(JoinFragLists) == 0 {
								JoinFragLists = append(JoinFragLists, []string{tableSite1 + "^" + tableSite2}) // [[customer.main^orders.segmenet2]]
							} else {
								JoinFragLists[0] = append(JoinFragLists[0], tableSite1+"^"+tableSite2) // [[customer.main^orders.segmenet2, customer.main^orders.segment3]]
							}
						}
					}
				}
			}
			// fmt.Println(JoinTableLists)
			// fmt.Println(JoinFragLists)
			// os.Exit(0)
			// JoinFragLists = append(JoinFragLists)
		} else {

			// step1: get all non-conflict join pairs under cond
			LeftRight := make(map[string][]string) // table1.Site1 : [table2.Site1, table2.Site2, ...]
			RightLeft := make(map[string][]string) // table2.Site1 : [table1.Site1, table1.Site2, ...]
			FragId1 := FragIdMap[table1]
			FragId2 := FragIdMap[table2]
			if !CheckConflictPossibility(table1, table2, col1, col2, &TmetaMap) { // no conflict
				// case1: vertical
				// case2: at least one column is not used for splitting fragments
				for site1 := range FragId1 { // main (customer)
					tableSite1 := table1 + "." + site1 // customer.main
					for site2 := range FragId2 {       // segment2, segment3
						tableSite2 := table2 + "." + site2 // orders.segment2
						LeftRight[tableSite1] = append(LeftRight[tableSite1], tableSite2)
						RightLeft[tableSite2] = append(RightLeft[tableSite2], tableSite1)
					}
				}
			} else { // horizontal
				Tmeta1 := TmetaMap[table1]
				Tmeta2 := TmetaMap[table2]
				for _, schema1 := range Tmeta1.FragSchema {
					tableSite1 := table1 + "." + schema1.SiteName
					for _, schema2 := range Tmeta2.FragSchema {
						tableSite2 := table2 + "." + schema2.SiteName
						var conflict = false
						// fmt.Println(i, j)
						for _, cond1 := range schema1.Conditions {
							if cond1.Col != col1 {
								continue
							}
							for _, cond2 := range schema2.Conditions {
								if cond2.Col != col2 {
									continue
								}
								conflict = ValueConflict(&cond1, &cond2)
								if conflict {
									break
								}
							}
							if conflict {
								break
							}
						}
						if !conflict {
							LeftRight[tableSite1] = append(LeftRight[tableSite1], tableSite2)
							RightLeft[tableSite2] = append(RightLeft[tableSite2], tableSite1)
						}
					}
				}
			}
			// fmt.Println("LeftRight=", LeftRight)
			// fmt.Println("RightLeft=", RightLeft)

			find := false
			for i, row := range JoinTableLists { // row = [customer, orders]

				// step2: find table in prior conditions
				fragLists := JoinFragLists[i] // [customer.main^orders.segmenet2, customer.main^orders.segment3]
				cntLength := len(fragLists)   // 2
				// fmt.Println("fragLists=", fragLists)
				isLeft := false
				index := -1
				for j, table := range row {
					if table == table1 {
						JoinTableLists[i] = append(JoinTableLists[i], table2)
						index = j
						isLeft = true
						break
					} else if table == table2 {
						JoinTableLists[i] = append(JoinTableLists[i], table1) // [customer, orders, book]
						index = j                                             // 1
						break
					}
				}

				// step3: merge and update join lists
				if index >= 0 { // find table in current row, merge join
					for _, list := range fragLists { // e.g. customer.main^orders.segmenet2
						// fmt.Println(j, list)
						frags := strings.Split(list, "^") // [customer.main, orders.segment2]
						tableSite := frags[index]         // orders.segment2
						if isLeft {
							for _, newFrag := range LeftRight[tableSite] {
								newList := list + "^" + newFrag
								JoinFragLists[i] = append(JoinFragLists[i], newList)
							}
						} else {
							for _, newFrag := range RightLeft[tableSite] {
								newList := list + "^" + newFrag
								JoinFragLists[i] = append(JoinFragLists[i], newList)
							}
						}
					}
					find = true
					JoinFragLists[i] = JoinFragLists[i][cntLength:]
				}
			}

			// step 4: if table not exists in current join lists, then create a new list
			if !find {
				JoinTableLists = append(JoinTableLists, []string{table1, table2})
				size := len(JoinFragLists)
				newList := make([]string, 0)
				JoinFragLists = append(JoinFragLists, newList)
				for left, rightList := range LeftRight {
					for _, right := range rightList {
						JoinFragLists[size] = append(JoinFragLists[size], left+"^"+right)
					}
				}
			}
		}
	}
	// fmt.Println(JoinTableLists)
	// fmt.Println(JoinFragLists)

	logicalPlanTree.CollectPatterns(&JoinTableLists, &JoinFragLists, &FragIdMap)

	// os.Exit(0)
}

func (logicalPlanTree *PlanTree) buildSelect(sel *sqlparser.Select) {
	if sel.From == nil {
		fmt.Println("cannot build plan tree without From")
		os.Exit(1)
	}
	// if len(sel.From) == 4 {
	// 	// println("handle 4 tables!!!!")
	// 	logicalPlanTree.buildBalanceTree()
	// } else {
	// 	for _, table := range sel.From {
	// 		tableName := sqlparser.String(table)
	// 		logicalPlanTree.AddTableNode(CreateTableNode(tableName))
	// 	}
	// }

	// get where string and join conditions
	whereString := ""
	var JoinConditions []string
	if sel.Where != nil {
		whereString = sqlparser.String(sel.Where.Expr)
		// fmt.Println("where string:", whereString)

		tmpString := whereString
		whereString = ""
		conditions := strings.Split(tmpString, "and")
		for _, cond := range conditions {
			cond = strings.ReplaceAll(cond, " ", "")
			operands := strings.FieldsFunc(cond, f)
			if len(operands) != 2 {
				continue
			}

			isJoinCond := false
			for i, oprd := range operands {
				if len(sel.From) == 1 && !CheckValue(oprd) {
					tableName := sqlparser.String(sel.From[0])
					if !strings.HasPrefix(oprd, tableName) {
						operands[i] = tableName + "." + operands[i]
					}
				} else if i == 1 && !CheckValue(oprd) { // means join condition
					isJoinCond = true
				}
			}

			op := strings.FieldsFunc(cond, f1)
			newCond := operands[0] + " " + op[0] + " " + operands[1]
			whereString += newCond + " and "
			if isJoinCond {
				JoinConditions = append(JoinConditions, newCond)
			}
		}
		whereString = strings.TrimSuffix(whereString, " and ")
		// fmt.Println("where string:", whereString)
		// fmt.Println("JoinConditions:", JoinConditions)
	}

	if sel.SelectExprs == nil {
		fmt.Println("cannot build plan tree without select")
		os.Exit(1)
	}

	projectionString := sqlparser.String(sel.SelectExprs)
	// fmt.Println("projection string = ", projectionString)

	if len(sel.From) == 1 {
		for _, table := range sel.From {
			tableName := sqlparser.String(table)
			Tmeta := FragmentFilter(tableName, projectionString, whereString)
			if len(Tmeta.FragSchema[0].Conditions) == 0 { // vertical
				logicalPlanTree.Root = logicalPlanTree.AddVerticalFragJoinNode(Tmeta)
			} else { // horizontal
				for i := int(0); i < Tmeta.FragNum; i++ {
					id := logicalPlanTree.addFragNode(createTableNode(tableName, Tmeta.FragSchema[i].SiteName))
					if i == 0 {
						logicalPlanTree.Root = id
					} else {
						logicalPlanTree.Root = logicalPlanTree.AddParentNode(CreateUnionNode(logicalPlanTree.GetTmpTableName()), logicalPlanTree.Root, id)
					}
				}
			}
		}
	} else {
		logicalPlanTree.GetJoinUnionOrders(sel, JoinConditions, projectionString, whereString)
	}

	// add selection node
	if sel.Where != nil {
		logicalPlanTree.AddFilternNode(CreateSelectionNode(logicalPlanTree.GetTmpTableName(), whereString))
	}
	// add projection node
	logicalPlanTree.AddFilternNode(CreateProjectionNode(logicalPlanTree.GetTmpTableName(), projectionString))
}

func Parse(sql string, txnID int64) *PlanTree {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// add rollback..
		fmt.Printf("parse error: %v\n", err.Error())
		return nil
	}

	planTree := new(PlanTree)
	planTree.InitialPlanTree(txnID)

	switch stmt.(type) {
	case *sqlparser.Select:
		planTree.buildSelect(stmt.(*sqlparser.Select))

	// case *sqlparser.Update:
	// 	// return handleUpdate(stmt.(*sqlparser.Update))
	case *sqlparser.Insert:
		fmt.Println(stmt.(*sqlparser.Insert))
		// 	// return handleInsert(stmt.(*sqlparser.Insert))
		// 	buildInsert(stmt.(*sqlparser.Insert))
		// case *sqlparser.Delete:
		// 	// return handleDelete(stmt.(*sqlparser.Delete))
	}

	// ips, ports, siteNames, err := storage.FetchSites("Publisher")
	// if err != nil {
	// 	return nil
	// }

	// // union node
	// node1 := new(UnionOperator)
	// node1.ip = "10.77.50.211"
	// node1.siteName = "main"

	// // union node
	// node2 := new(UnionOperator)
	// node2.ip = "10.77.50.211"
	// node2.siteName = "main"
	// node1.left = node2
	// planTree.nodeNum = 1

	// // union node
	// node3 := new(UnionOperator)
	// node3.ip = "10.77.50.211"
	// node2.siteName = "main"
	// node1.right = node3

	// // scan node
	// node4 := new(ScanOperator)
	// node4.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node4.ip = ips[0]
	// node4.port = ports[0]
	// node4.tableName = "Publisher"
	// node4.siteName = siteNames[0]
	// node4.left = nil
	// node4.right = nil

	// // scan node
	// node5 := new(ScanOperator)
	// node5.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node5.ip = ips[1]
	// node5.port = ports[1]
	// node5.tableName = "Publisher"
	// node5.siteName = siteNames[1]
	// node5.left = nil
	// node5.right = nil

	// // scan node
	// node6 := new(ScanOperator)
	// node6.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node6.ip = ips[2]
	// node6.port = ports[2]
	// node6.tableName = "Publisher"
	// node6.siteName = siteNames[2]
	// node6.left = nil
	// node6.right = nil

	// // scan node
	// node7 := new(ScanOperator)
	// node7.db = storage.NewDb("postgres", "postgres", "postgres", 5555, "disable")
	// node7.ip = ips[3]
	// node7.port = ports[3]
	// node7.tableName = "Publisher"
	// node7.siteName = siteNames[3]
	// node7.left = nil
	// node7.right = nil

	// node2.left = node4
	// node2.right = node5
	// node3.left = node6
	// node3.right = node7

	// planTree.root = node1

	return planTree
}
