---
alwaysApply: false
---
📋 模块实现蓝图：智能初始化与图算法决策树 (Smart Initialization & Graph Routing)
1. 背景与核心思想 (Background & Core Concept)
本模块是 Text-to-SQL Schema Linking Pipeline 的第一阶段（Initialization Phase）。
● 痛点：传统方法让 LLM 直接在数据库图中“一步步游走探索”，极易产生幻觉、断路，且消耗大量 Token。此外，盲目连通多个表容易引发“斯坦纳树毒化 (Steiner Tree Poisoning)”，引入大量无关的噪声表。
● 解决方案 (Neuro-Symbolic)：
  a. LLM 负责找点：利用规则和大模型，精准提取问题涉及的核心锚点表 (Anchor Tables)。
  b. 图算法负责铺路与分流：底层的 NetworkX 引擎根据锚点数量 ($N$) 自动计算拓扑连通性，并通过严格的决策树 (Decision Tree) 将任务分流。只有在物理拓扑产生歧义时，才将复杂决策推迟给下一阶段的 LLM。
2. 核心输入与依赖 (Inputs & Dependencies)
● G (NetworkX DiGraph)：全库的 Schema 图结构（节点为 Table/Column，边为 FOREIGN_KEY 等）。计算连通性时，必须使用其无向视图 G.to_undirected()，因为 SQL 的 JOIN 是无向的。
● question (String)：用户的自然语言问题。
● llm_client (Object)：大模型调用接口。
3. 算法决策树完整流程 (The Decision Tree Algorithm)
主控函数：initialize_subgraph(question, graph)
Step 1: 获取初始锚点表 (Identify Anchors)
通过预先定义的 LLM 逻辑或双 Agent 流程（规则匹配 + 语义清洗），提取出问题最核心的表级锚点集合：
$T_{initial} = [Table_1, Table_2, \dots, Table_N]$
Step 2: 核心决策路由 (Graph Algorithm Routing)
根据锚点数量 $N$ 执行分支逻辑：
🌿 分支 0：$N = 0$ (异常拦截)
● 动作：无锚点。
● 返回：{"status": "failed", "reason": "No initial tables found."}
🌿 分支 1：$N = 1$ (单表直通)
● 动作：用户查询仅涉及单张表，无需连通。
● 返回：{"status": "fast_track", "subgraph_nodes": T_initial, "message": "Single table query."}
🌿 分支 2：$N = 2$ (双表寻路)
● 动作：使用 nx.all_shortest_paths 在无向图上寻找这两个表之间的所有最短路径。
  ○ 子分支 2.A (唯一通途)：如果仅找到 1 条最短路径。
    ■ 说明物理结构无歧义。
    ■ 返回：{"status": "fast_track", "subgraph_nodes": path_nodes, "message": "Unique shortest path found."}
  ○ 子分支 2.B (歧义探索)：如果找到 > 1 条最短路径。
    ■ 说明存在多种等效的 JOIN 方式（如既可以通过“选课”关联，也可通过“助教”关联）。
    ■ 提取这些路径的拓扑节点序列打包为线索。
    ■ 返回：{"status": "ambiguity_needs_resolution", "anchors": T_initial, "path_clues": [path1, path2...], "message": "Multiple shortest paths exist, semantic resolution needed."}
🌿 分支 3：$N \ge 3$ (多表斯坦纳树与毒化防御)
● 动作：多约束条件下，使用近似斯坦纳树算法 nx.algorithms.approximation.steiner_tree 将所有锚点连通为一棵最小权重树。
● 防御机制 (Steiner Tree Poisoning Check)：
  ○ 计算算法额外引入的中间表数量：extra_nodes_count = len(tree_nodes) - len(T_initial)。
  ○ 子分支 3.A (结构紧凑 - 安全)：如果 extra_nodes_count <= 3 (阈值可配)。
    ■ 说明锚点间关联紧密，拓扑结构唯一且可靠。
    ■ 返回：{"status": "fast_track", "subgraph_nodes": tree_nodes, "message": "Steiner tree connected safely."}
  ○ 子分支 3.B (过度延伸 - 毒化警报)：如果 extra_nodes_count > 3。
    ■ 说明为了强行连通这些表，图算法引入了大量无关中间表。大概率是 Step 1 选错了毫无关联的“毒瘤”锚点。
    ■ 返回：{"status": "toxic_anchors_detected", "anchors": T_initial, "tree_nodes": tree_nodes, "message": "Too many intermediate nodes required. Suspected poisoning."}
4. 给 AI Assistant 的代码实现要求 (Coding Guidelines)
1. 解耦设计：在anchor_selectior.py创建一个专门的类来实现上述逻辑。不要在此类中实现任何“向子图里添加 Column 列”的逻辑（那是下一个阶段的任务）。
2. 纯函数与状态机：方法的返回值必须是严格定义的 dict（如上述返回示例），通过 status 字段 (fast_track, ambiguity_needs_resolution, toxic_anchors_detected, failed) 驱动外部主干 Pipeline 进行下一步。
3. 异常处理：在调用 nx.all_shortest_paths 或 steiner_tree 时，如果抛出 nx.NetworkXNoPath 或相关异常，需捕获并返回 {"status": "failed", "reason": "Nodes are physically disconnected in the graph."}。
4. 如果出现上述歧义情况，再次交给LLM处理，从而得到成锚点选择sl1最终结果。
5. 同时要在可视化系统上进行可视化（在原来的基础上新增分支决策步骤），保留原来的锚点初步选择结果，并显示分支决策类型和结果。