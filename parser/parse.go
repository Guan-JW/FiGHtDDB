package parser

import (
	"fmt"

	"github.com/pingcap/tidb/parser"
)

type PlanTreeNode struct {

}

type PlanTree struct {
	nodeNum int
	root 	*PlanTreeNode
}

func Parse(sql string) *PlanTree {
	query := queryCreate(sql)
	query.attrname = append(query.attrname, "xxx")

	planTree := new(PlanTree)
	planTree.nodeNum = 0
	planTree.root = nil

	return planTree
}

func test() {
	// 0. make sure import parser_driver implemented by TiDB(user also can implement own driver by self).
	// and add `import _ "github.com/pingcap/tidb/types/parser_driver"` in the head of file.

	// 1. Create a parser. The parser is NOT goroutine safe and should
	// not be shared among multiple goroutines. However, parser is also
	// heavy, so each goroutine should reuse its own local instance if
	// possible.
	p := parser.New()

	// 2. Parse a text SQL into AST([]ast.StmtNode).
	stmtNodes, _, err := p.Parse("select * from tbl where id = 1", "", "")

	// 3. Use AST to do cool things.
	fmt.Println(stmtNodes[0], err)
}