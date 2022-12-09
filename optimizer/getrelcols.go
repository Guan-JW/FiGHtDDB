package optimizer

import (
	"fmt"
	"strings"

	"github.com/FiGHtDDB/parser"
	"github.com/FiGHtDDB/storage"
)

func GetRelCols(pt *parser.PlanTree) *parser.PlanTree {
	// var newtree parser.PlanTree
	getRelCols(pt, pt.Root)
	// newtree = oldtree
	// newtree.Print()
	return pt
}

func getRelCols(t *parser.PlanTree, n int64) {
	//if invalid Nodeid
	node := &t.Nodes[n]
	// println("!!!!", node.Nodeid)
	if node.Nodeid == -1 {
		return
	}
	//if access leaf node
	if node.Left == -1 && node.Right == -1 {
		getleafcols(node)
		// println("Rel_cols of table [", node.TmpTable, "] is: ", node.Rel_cols)
	}
	//if only n.Left exist
	if node.Left != -1 && node.Right == -1 {
		// println("only left")
		getRelCols(t, node.Left)
		node.Rel_cols = t.Nodes[node.Left].Rel_cols
		// println("only left: ", node.Rel_cols)
	}
	//if only n.Right exist
	if node.Right != -1 && node.Left == -1 {
		// println("only right")
		getRelCols(t, node.Right)
		node.Rel_cols = t.Nodes[node.Right].Rel_cols
		// println("only right: ", node.Rel_cols)
	}

	if node.Left != -1 && node.Right != -1 {
		// println("double")
		getRelCols(t, node.Left)
		// println("left: ", t.Nodes[node.Left].Rel_cols)
		getRelCols(t, node.Right)
		// println("right: ", t.Nodes[node.Right].Rel_cols)
		node.Rel_cols = union(t.Nodes[node.Left].Rel_cols, t.Nodes[node.Right].Rel_cols)
		// println(node.Rel_cols)
	}
}

func getleafcols(node *parser.PlanTreeNode) {
	Tmeta, err := storage.GetTableMeta(node.TmpTable)
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(Tmeta.FragSchema[0].Conditions) != 0 { // horizontal
		// node.Rel_cols = Tmeta.FragSchema[0].Cols
		node.Rel_cols = ""
		for _, col := range Tmeta.FragSchema[0].Cols {
			node.Rel_cols += node.TmpTable + "." + col + ","
		}
		node.Rel_cols = strings.TrimSuffix(node.Rel_cols, ",")
		// node.Rel_cols = strings.Join(Tmeta.FragSchema[0].Cols, ",")
	} else { // vertical
		node.Rel_cols = ""
		for _, schema := range Tmeta.FragSchema {
			if node.Locate == schema.SiteName {
				for _, col := range schema.Cols {
					node.Rel_cols += node.TmpTable + "." + col + ","
				}
				break
			}
		}
		node.Rel_cols = strings.TrimSuffix(node.Rel_cols, ",")
	}
}

func union(str1 string, str2 string) string {
	f := func(c rune) bool {
		return c == ','
	}
	// println(str1)
	// println(str2)
	arr1 := strings.FieldsFunc(str1, f)
	arr2 := strings.FieldsFunc(str2, f)
	var arr3 []string
	for _, str := range arr1 {
		arr3 = append(arr3, str)
	}
	for _, str := range arr2 {
		if notExist(arr3, str) {
			// println(arr3, str)
			arr3 = append(arr3, str)
		}
	}
	result := ""
	length := len(arr3)
	for i, str := range arr3 {
		result += str
		if i < length-1 {
			result += ","
		}
	}

	return result
}

func notExist(arr []string, s string) bool {
	for _, str := range arr {
		if strings.EqualFold(str, s) {
			return false
		}
	}
	return true
}
