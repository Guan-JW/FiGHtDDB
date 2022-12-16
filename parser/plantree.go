package parser

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

const (
	//MaxNodeNum define the max num of the plantree nodes
	MaxNodeNum     = 1000
	MaxFragNum     = 10
	MaxFragCondNum = 5 // number of condition on the same attribute
)

type PlanTreeNode struct {
	Nodeid       int64  // node id
	Left         int64  // left child id
	Right        int64  // right child id
	Parent       int64  // parent id
	Status       int64  // 1 for finished, 0 for waiting
	TmpTable     string // the name of tmp_table
	Locate       string // site name
	TransferFlag bool   // 1 for transer, 0 for not
	Dest         string // the site name of the dest
	NodeType     int64  // 1 for table, 2 for select, 3 for projection, 4 for join, 5 for union, 6 for insert, 7 for delete
	//detail string//according to node_type, (1)table_name for table, (2)where_condition for select, (3)col_name for projection, (4)join_type for join, (5)nil for union
	Where         string
	Rel_cols      string // split(",",s)   !!!!
	Cols          string // attributes used in the rest of the tree
	ExecStmtCols  string // attributes used to construct execution stmts -- TmpTable.col
	ExecStmtWhere string // where used to construct execution stmts -- TmpTable.col

	//cols_type string
	// Join_type int64  //0 for x, 1 for =, 2 for natural
	// Join_cols string //"customer_id,id"
	//union string
}

type PlanTree struct {
	TxnID       int64 // transaction id
	NodeNum     int64 // total number of tree nodes
	Root        int64 // index of the root node
	TmpTableNum int64
	Nodes       [MaxNodeNum]PlanTreeNode
}

type FragCond struct {
	CondNum int
	Op      [MaxFragCondNum]string // 0 for =, 1 for <, 2 for >, 3 for <=, 4 for >=
	Value   [MaxFragCondNum]string // value
}

type FragNode struct {
	FragId int64
	// FragName      string //
	FragCondition map[string]FragCond
	SiteNum       int64
	Ip            string
}

type FragTree struct {
	FragNum   int64 // #Fragments
	FragType  int64 //0 for Horizontal, 1 for vertical
	TableName string
	Frags     [MaxFragNum]FragNode
}

func (pt *PlanTree) Print() {
	fmt.Println("root is: ", pt.Root)
	for _, node := range pt.Nodes {
		if node.Nodeid == -1 || node.Nodeid == 0 {
			continue
		}
		// fmt.Println(node)
		fmt.Println("Id=", node.Nodeid, "; Status:", node.Status, "; Type:", node.NodeType, ";Locate", node.Locate, "; where:", node.Where, "; Rel_cols:", node.Rel_cols, "; Cols:", node.Cols, "; TmpTable:", node.TmpTable, "; Left:", node.Left, ";Right:", node.Right, ";Parent:", node.Parent)
	}
	return
}

func InitialPlanTreeNode() (node PlanTreeNode) {
	node.Nodeid = -1
	node.Left = -1
	node.Right = -1
	node.Parent = -1
	node.Status = -1
	node.TmpTable = ""
	node.Locate = ""
	node.TransferFlag = false
	node.Dest = ""
	node.NodeType = -1
	node.Where = ""
	node.Rel_cols = ""
	node.Cols = ""
	node.ExecStmtCols = ""
	node.ExecStmtWhere = ""
	// node.Joint_type = -1
	// node.Joint_cols = ""
	return node
}

func (planTree *PlanTree) InitialPlanTree(txnID int64) {
	for i := 0; i < MaxNodeNum; i++ {
		planTree.Nodes[i] = InitialPlanTreeNode()
	}
	planTree.Root = -1
	planTree.NodeNum = 0
	planTree.TxnID = txnID
	planTree.TmpTableNum = 0
}

// findEmptyNode returns the idx of first empty node
func (pt *PlanTree) findEmptyNode() int64 {
	for i, node := range pt.Nodes {
		if i != 0 && node.Nodeid == -1 {
			return int64(i)
		}
	}
	println("Error when creating node, no empty node left!")
	return -1
}

// GetTmpTableName can get latest TmpTableName
func (pt *PlanTree) GetTmpTableName() (TmpTableName string) {
	TmpTableName = "_Transaction_" + fmt.Sprintf("%d", pt.TxnID) + "_TmpTable_" + fmt.Sprintf("%d", pt.TmpTableNum)
	pt.TmpTableNum++
	return TmpTableName
}

func (pt *PlanTree) DrawTreeNode(graph *cgraph.Graph, node_id int64, id *int) *cgraph.Node {
	node := pt.Nodes[node_id]
	n, err := graph.CreateNode(node.TmpTable + "-" + strconv.Itoa(*id))
	*id++
	// fmt.Println("Cols = ", node.Cols)
	// fmt.Println("Where = ", node.Where)
	// fmt.Println("ExecCols = ", node.ExecStmtCols)
	// fmt.Println("ExecWhere = ", node.ExecStmtWhere)
	if err != nil {
		log.Fatal(err)
		// do something with error
	}
	switch node.NodeType {
	case 1:
		//table node -- leaf
		// if node.Cols == "" {
		// 	n.SetLabel("Project: *\n" + node.TmpTable)
		// } else {
		// 	n.SetLabel("Project: " + node.Cols + "\n" + node.TmpTable)
		// }
		graph.DeleteNode(n)
		createString := node.TmpTable + "\n" + node.Locate
		if node.Left != -1 {
			createString = pt.Nodes[node.Left].TmpTable + "\n" + node.Locate
		}
		if node.TransferFlag {
			createString += "\nTransfer"
		}
		n, err = graph.CreateNode(createString + "-" + strconv.Itoa(*id))
		n.SetLabel(createString)
		*id++

		if err != nil {
			log.Fatal(err)
			// do something with error
		}
		// fmt.Println("Table node: ", node.TmpTable, "; SiteName: ", node.Locate)
		// n.SetLabel(node_id)

	case 2:
		//select
		labelString := ""
		if node.Cols == "" {
			labelString = "Select: select *\n" + node.Where
		} else {
			labelString = "Select: select " + node.Cols + "\n" + node.Where
		}
		if node.TransferFlag {
			labelString += "\nTransfer"
		}
		n.SetLabel(labelString)
		if node.Left != -1 {
			left_node := pt.DrawTreeNode(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNode(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	case 3:
		//projection
		label := "Project: "
		if node.Cols == "" {
			label += "select *\n"
		} else {
			label += "select " + node.Cols + "\n"
		}
		if node.TransferFlag {
			label += "\nTransfer"
		}

		n.SetLabel(label)
		if node.Left != -1 {
			left_node := pt.DrawTreeNode(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
	case 4:
		// join
		label := "Join: "
		if node.Cols == "" {
			label += "select *\n"
		} else {
			label += "select " + node.Cols + "\n"
		}

		if node.Where == "" {
			label += "Equal Join"
		} else {
			label += node.Where
		}
		if node.TransferFlag {
			label += "\nTransfer"
		}
		n.SetLabel(label)

		if node.Left != -1 {
			left_node := pt.DrawTreeNode(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNode(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	case 5:
		//union
		label := "Union"
		// if node.Cols == "" {
		// 	label += "select *\n"
		// } else {
		// 	label += "select " + node.Cols + "\n"
		// }
		if node.TransferFlag {
			label += "\nTransfer"
		}

		n.SetLabel(label)

		if node.Left != -1 {
			left_node := pt.DrawTreeNode(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNode(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	case 7:
		label := "Delete\n" + "delete from " + node.TmpTable + "\n" + node.ExecStmtWhere + "\nLocate: " + node.Locate
		n.SetLabel(label)
		if node.Left != -1 {
			left_node := pt.DrawTreeNode(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNode(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	default:
		// should never reach here
	}

	switch node.Locate {
	case "main":
		n.SetColor("red")
	case "segment1":
		n.SetColor("yellow")
	case "segment2":
		n.SetColor("green")
	case "segment3":
		n.SetColor("blue")
	}
	return n
}

func (planTree *PlanTree) DrawPlanTree(query_id int, postfix string) {
	if planTree.Root < 0 {
		return
	}

	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()

	if planTree.Nodes[planTree.Root].NodeType == 6 { // insert
		for i := planTree.Root; i <= int64(planTree.NodeNum); i++ {
			node := planTree.Nodes[i]
			_, err := graph.CreateNode("insert into " + node.TmpTable + " (" + node.ExecStmtCols + ")\nvalues (" + node.Cols + ")\nSite: " + node.Locate)
			if err != nil {
				log.Fatal(err)
				// do something with error
			}
		}
	} else if planTree.Nodes[planTree.Root].NodeType == 7 { // delete
		id := 0
		for i := int64(1); i <= planTree.NodeNum; i++ {
			if planTree.Nodes[i].NodeType == 7 {
				planTree.DrawTreeNode(graph, i, &id)
			}
		}
	} else {
		id := 0
		planTree.DrawTreeNode(graph, planTree.Root, &id)
	}

	// 1. write encoded PNG data to buffer
	var buf bytes.Buffer
	if err := g.Render(graph, graphviz.PNG, &buf); err != nil {
		log.Fatal(err)
	}
	// 2. get as image.Image instance
	g.RenderImage(graph)
	if err != nil {
		log.Fatal(err)
	}

	// 3. write to file directly
	path := "./TreeImages/"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
	path += "query_" + strconv.Itoa(query_id) + "/"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
	if err := g.RenderFilename(graph, graphviz.PNG, path+"query_"+strconv.Itoa(query_id)+"_"+postfix+".png"); err != nil {
		log.Fatal(err)
	}
}

func (pt *PlanTree) DrawTreeNodeTmpTable(graph *cgraph.Graph, node_id int64, id *int) *cgraph.Node {
	node := pt.Nodes[node_id]
	n, err := graph.CreateNode(node.TmpTable + "-" + strconv.Itoa(*id))
	*id++
	if err != nil {
		log.Fatal(err)
		// do something with error
	}
	switch node.NodeType {
	case 1:
		//table node -- leaf
		// if node.ExecStmtCols == "" {
		// 	n.SetLabel("Project: *\n" + node.TmpTable)
		// } else {
		// 	n.SetLabel("Project: " + node.ExecStmtCols + "\n" + node.TmpTable)
		// }
		graph.DeleteNode(n)
		createString := node.TmpTable + "\n" + node.Locate
		if node.Left != -1 {
			createString = "Create TmpTable: " + node.TmpTable + "\nFrom: " + pt.Nodes[node.Left].TmpTable
		}
		if node.TransferFlag {
			createString += "\nTransfer"
		}
		n, err = graph.CreateNode(createString + "-" + strconv.Itoa(*id))
		n.SetLabel(createString)
		*id++

		if err != nil {
			log.Fatal(err)
			// do something with error
		}
		// fmt.Println("Table node: ", node.TmpTable, "; SiteName: ", node.Locate)
		// n.SetLabel(node_id)

	case 2:
		//select
		label := "Select: "
		if node.ExecStmtCols == "" {
			label += "select *\n" + node.ExecStmtWhere + "\n TmpTable: " + node.TmpTable
		} else {
			label += "select " + node.ExecStmtCols + "\n" + node.ExecStmtWhere + "\n TmpTable: " + node.TmpTable
		}
		if node.TransferFlag {
			label += "\nTransfer"
		}
		n.SetLabel(label)

		if node.Left != -1 {
			left_node := pt.DrawTreeNodeTmpTable(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNodeTmpTable(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	case 3:
		//projection
		label := "Project: "
		if node.ExecStmtCols == "" {
			label += "select *\nTmpTable: " + node.TmpTable
		} else {
			label += "select " + node.ExecStmtCols + "\nTmpTable: " + node.TmpTable
		}
		if node.TransferFlag {
			label += "\nTransfer"
		}
		n.SetLabel(label)
		if node.Left != -1 {
			left_node := pt.DrawTreeNodeTmpTable(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
	case 4:
		// join
		label := "Join: "
		if node.ExecStmtCols == "" {
			label += "select *\n"
		} else {
			label += "select " + node.ExecStmtCols + "\n"
		}

		if node.ExecStmtWhere == "" {
			label += "Equal Join\nTmpTable: " + node.TmpTable
		} else {
			label += node.ExecStmtWhere + "\nTmpTable: " + node.TmpTable
		}
		if node.TransferFlag {
			label += "\nTransfer"
		}
		n.SetLabel(label)

		if node.Left != -1 {
			left_node := pt.DrawTreeNodeTmpTable(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNodeTmpTable(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	case 5:
		//union
		label := "Union \nTmpTable: " + node.TmpTable
		// if node.ExecStmtCols == "" {
		// 	label += "select *\nTmpTable: " + node.TmpTable
		// } else {
		// 	label += "select " + node.ExecStmtCols + "\nTmpTable: " + node.TmpTable
		// }
		if node.TransferFlag {
			label += "\nTransfer"
		}
		n.SetLabel(label)

		if node.Left != -1 {
			left_node := pt.DrawTreeNodeTmpTable(graph, node.Left, id)
			// left_node, _ := graph.CreateNode(node.TmpTable)
			// left_node.SetLabel("left")
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNodeTmpTable(graph, node.Right, id)
			// right_node, _ := graph.CreateNode(node.TmpTable)
			// right_node.SetLabel("left")
			graph.CreateEdge("", n, right_node)
		}

	case 7:
		label := "Delete\n" + "delete from " + node.TmpTable + "\n" + node.ExecStmtWhere + "\nLocate: " + node.Locate
		n.SetLabel(label)
		if node.Left != -1 {
			left_node := pt.DrawTreeNodeTmpTable(graph, node.Left, id)
			graph.CreateEdge("", n, left_node)
		}
		if node.Right != -1 {
			right_node := pt.DrawTreeNodeTmpTable(graph, node.Right, id)
			graph.CreateEdge("", n, right_node)
		}
	default:
		// should never reach here
	}

	switch node.Locate {
	case "main":
		n.SetColor("red")
	case "segment1":
		n.SetColor("yellow")
	case "segment2":
		n.SetColor("green")
	case "segment3":
		n.SetColor("blue")
	}
	return n
}

func (planTree *PlanTree) DrawPlanTreeTmpTable(query_id int, postfix string) {
	if planTree.Root < 0 { // nothing to draw
		return
	}
	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()

	if planTree.Nodes[planTree.Root].NodeType == 6 { // insert
		for i := int64(1); i <= int64(planTree.NodeNum); i++ {
			node := planTree.Nodes[i]
			_, err := graph.CreateNode("insert into " + node.TmpTable + " (" + node.ExecStmtCols + ")\nvalues (" + node.Cols + ")\nSite: " + node.Locate)
			if err != nil {
				log.Fatal(err)
				// do something with error
			}
		}
	} else if planTree.Nodes[planTree.Root].NodeType == 7 { // delete
		id := 0
		for i := int64(1); i <= planTree.NodeNum; i++ {
			if planTree.Nodes[i].NodeType == 7 {
				planTree.DrawTreeNodeTmpTable(graph, i, &id)
			}
		}
	} else {
		id := 0
		planTree.DrawTreeNodeTmpTable(graph, planTree.Root, &id)
	}

	// 1. write encoded PNG data to buffer
	var buf bytes.Buffer
	if err := g.Render(graph, graphviz.PNG, &buf); err != nil {
		log.Fatal(err)
	}
	// 2. get as image.Image instance
	g.RenderImage(graph)
	if err != nil {
		log.Fatal(err)
	}

	// 3. write to file directly
	path := "./TreeImages/"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
	path += "query_" + strconv.Itoa(query_id) + "/"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
	if err := g.RenderFilename(graph, graphviz.PNG, path+"query_"+strconv.Itoa(query_id)+"_"+postfix+".png"); err != nil {
		log.Fatal(err)
	}
}

type Condition struct {
	Col   string
	Type  string
	Comp  string
	Value string
}

type FragSchema struct {
	SiteName   string
	Cols       []string
	Conditions []Condition
}

type TableMeta struct {
	TableName  string
	FragNum    int
	FragSchema []FragSchema
}

func GetFixFragMeta(TableName string) *TableMeta {
	switch TableName {
	case "publisher":
		PubMeta := new(TableMeta)
		PubMeta.TableName = TableName
		PubMeta.FragNum = 4
		PubMeta.FragSchema = make([]FragSchema, 4)

		frag := new(FragSchema)
		frag.SiteName = "main"
		frag.Cols = []string{"id", "name", "nation"}
		frag.Conditions = []Condition{{"id", "int", "<", "104000"}, {"nation", "string", "=", "'PRC'"}}
		PubMeta.FragSchema[0] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment1"
		frag.Cols = []string{"id", "name", "nation"}
		frag.Conditions = []Condition{{"id", "int", "<", "104000"}, {"nation", "string", "=", "'USA'"}}
		PubMeta.FragSchema[1] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment2"
		frag.Cols = []string{"id", "name", "nation"}
		frag.Conditions = []Condition{{"id", "int", ">=", "104000"}, {"nation", "string", "=", "'PRC'"}}
		PubMeta.FragSchema[2] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment3"
		frag.Cols = []string{"id", "name", "nation"}
		frag.Conditions = []Condition{{"id", "int", ">=", "104000"}, {"nation", "string", "=", "'USA'"}}
		PubMeta.FragSchema[3] = *frag

		return PubMeta

	case "book":
		BookMeta := new(TableMeta)
		BookMeta.TableName = TableName
		BookMeta.FragNum = 3
		BookMeta.FragSchema = make([]FragSchema, 3)

		frag := new(FragSchema)
		frag.SiteName = "main"
		frag.Cols = []string{"id", "title", "authors", "publisher_id", "copies"}
		frag.Conditions = []Condition{{"id", "int", "<", "205000"}}
		BookMeta.FragSchema[0] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment1"
		frag.Cols = []string{"id", "title", "authors", "publisher_id", "copies"}
		frag.Conditions = []Condition{{"id", "int", ">=", "205000"}, {"id", "int", "<", "210000"}}
		BookMeta.FragSchema[1] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment2"
		frag.Cols = []string{"id", "title", "authors", "publisher_id", "copies"}
		frag.Conditions = []Condition{{"id", "int", ">=", "210000"}}
		BookMeta.FragSchema[2] = *frag
		return BookMeta

	case "customer":
		CusMeta := new(TableMeta)
		CusMeta.TableName = TableName
		CusMeta.FragNum = 2
		CusMeta.FragSchema = make([]FragSchema, 2)

		frag := new(FragSchema)
		frag.SiteName = "main"
		frag.Cols = []string{"id", "name"}
		CusMeta.FragSchema[0] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment1"
		frag.Cols = []string{"id", "rank"}
		CusMeta.FragSchema[1] = *frag
		return CusMeta
	case "orders":
		OrderMeta := new(TableMeta)
		OrderMeta.TableName = TableName
		OrderMeta.FragNum = 4
		OrderMeta.FragSchema = make([]FragSchema, 4)

		frag := new(FragSchema)
		frag.SiteName = "main"
		frag.Cols = []string{"customer_id", "book_id", "quantity"}
		frag.Conditions = []Condition{{"customer_id", "int", "<", "307000"}, {"book_id", "int", "<", "215000"}}
		OrderMeta.FragSchema[0] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment1"
		frag.Cols = []string{"customer_id", "book_id", "quantity"}
		frag.Conditions = []Condition{{"customer_id", "int", "<", "307000"}, {"book_id", "int", ">=", "215000"}}
		OrderMeta.FragSchema[1] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment2"
		frag.Cols = []string{"customer_id", "book_id", "quantity"}
		frag.Conditions = []Condition{{"customer_id", "int", ">=", "307000"}, {"book_id", "int", "<", "215000"}}
		OrderMeta.FragSchema[2] = *frag

		frag = new(FragSchema)
		frag.SiteName = "segment3"
		frag.Cols = []string{"customer_id", "book_id", "quantity"}
		frag.Conditions = []Condition{{"customer_id", "int", ">=", "307000"}, {"book_id", "int", ">=", "215000"}}
		OrderMeta.FragSchema[3] = *frag
		return OrderMeta
	}
	return nil
}
