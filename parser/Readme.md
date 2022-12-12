# Parser

### **目录结构**

1. `../Parser/`: sql解析、查询树生成；
2. `../Optimize/`: 查询树优化；
3. `test_parser.go`: 执行11条sql查询，生成查询计划;
4. `TreeImages/`: 11条sql对应的 PlanTree
   1. `TreeImages/query_*/query_*_0.png`: 未经过优化的基础 PlanTree；
   2. `TreeImages/query_*/query_*_1.png`: 加入分区表的 PlanTree（根据查询条件完成了分区裁剪）；
   3. `TreeImages/query_*/query_*_2.png`: 加入选择下推等优化后的 PlanTree；
   4. `TreeImages/query_*/query_*_tmp.png`: 以临时表展示的 PlanTree。

### **使用**

参考`test_parser.go`，调用如下三个函数，获得最终查询树：

```go
planTree := parser.Parse(sqlString, txnID)
planTree.Analyze()
planTree = optimizer.Optimize(planTree)
```

#### **注意：**

1. **元数据**: parser.Parse 函数需要传入 Transaction ID 参数，用于编排唯一的临时表名，需要从元数据中获取（？）；

2. **执行**：

   1. 节点分为5个类型(NodeType)：1-table、2-selection、3-projection、4-join、5-union。
   2. 节点执行时可以通过结构中的 ExecStmtCols 和 ExecStmtWhere 构建 select 语句（可以参考`parser/plantree.go: DrawTreeNodeTmpTable()`函数和PlanTree的图像`TreeImages/query_*/query_*_tmp.png`），操作临时表。
      1. table 节点只做传输；
      2. selection 节点通过 ExecStmtCols 和 ExecStmtWhere 构建 select 语句；
      3. projection 节点通过 ExecStmtCols 构建 select 语句；
      4. join 节点通过 ExecStmtCols 和 ExecStmtWhere 构建 select 语句，如果 Join 节点的 ExecStmtWhere == ""，则表示执行笛卡尔积；
      5. union 节点不过滤，直接 select * from A uion select * from B 即可。
      6. *注意*：如果 ExecStmtCols == ""，则表示 select *；

   *总之 table 节点和 union 节点不需要 ExecStmtCols 和 ExecStmtWhere。*

   