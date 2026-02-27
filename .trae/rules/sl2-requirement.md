---
alwaysApply: false
---



# 📋 模块实现蓝图：Phase 2 - 迭代式子图扩展与终止判断

## 1. 核心设计理念 (Core Philosophy)

* **起点**：一个经过图算法严格验证连通性的**核心子图 (Core Subgraph)**（包含表节点及其强制绑定的桥梁外键列）。
* **动作融合**：在每一个迭代轮次中，LLM 需要同时完成三件事：
1. **填肉 (Column Selection)**：从当前已确认的核心子图表中，挑选业务所需的列。
2. **探边 (1-Hop Expansion)**：审视核心子图向外延伸的 1-hop 邻居表，判断是否需要拉入新的表。
3. **判决 (Termination Check)**：判断当前掌握的信息是否足以回答问题（`is_solvable`）。


* **状态继承**：如果未终止，被选中的 1-hop 邻居表将被并入“核心子图”，系统重新计算新的 1-hop 边界，进入下一轮。为了防止死循环，需设置最大迭代次数（如 `max_iterations = 3`）。

## 2. 状态机流转逻辑 (State Machine Logic)

主控逻辑可以封装在一个 `IterativeSubgraphExpander` 类中。

### **Step 1: 状态初始化 (State Prep)**

接收来自上一阶段的表列表 `initial_tables` (例如 `["Student", "Enrollment"]`)。

* `Current_Core_Tables` = `initial_tables`
* `Iteration_Count` = 0

### **Step 2: 构建本轮上下文 (Build Context per Round)**

每一次进入循环，系统通过 NetworkX 图 `G` 自动计算两部分信息：

1. **内部资产 (Inner Assets)**：
* 找出 `Current_Core_Tables` 内部互相连接的**强制桥梁列 (Mandatory Keys)**。如果两表之间有多个强制桥梁列，先全选让LLM根据业务逻辑判断选择哪一个。
* 提取这些表内部的**所有候选列 (Candidate Columns)** 及统计特征（Type, Samples）。（其实就是列的FUll模式）


2. **外部边界 (Frontier 1-Hop)**：
* 在无向图视图中，找出所有与 `Current_Core_Tables` 距离恰好为 1 跳的**邻居表 (Neighbor Tables)**。
* 提取这些邻居表的简要描述（表名、主键、关联关系）。（brief模式）



### **Step 3: 构造合一的 Prompt 并调用 LLM**

将“内部资产”和“外部边界”使用本项目自定义接口的schema动态信息展示喂给 LLM，并附带上一轮的推理结论（如果有）。


### **Step 4: LLM 响应解析与动作执行 (Action Execution)**

LLM 的预期输出格式（严格 JSON）：

```json
{
  "selected_columns_from_core": {
    "Student": ["name"],
    "Enrollment": ["grade"]
  },
  "selected_tables_from_frontier": ["Course"],
  "to_solve_the_question": {
    "is_solvable": false,
    "reasoning": "I have the student name and grade, but I need the 'Course' table to filter by the course title 'Database'."
  }
}

```

**系统逻辑判断：**

* **If `is_solvable == True` OR `Iteration_Count >= MAX_LIMIT**`:
* **终止循环**。
* 合成最终子图：保留 `Current_Core_Tables` + `selected_columns_from_core` + 强制桥梁列。
* 返回生成的 NetworkX 子图。


* **If `is_solvable == False**`:
* 更新状态：将 `selected_tables_from_frontier` 并入 `Current_Core_Tables`。
* 提取 LLM 的 `reasoning` 作为下一轮的 `result_from_last_round`。
* `Iteration_Count += 1`，回到 **Step 2**。



---

## 3. 给 AI Assistant 的代码编写指令 (For Cursor)

你可以将以下提示词直接发送给 Cursor 来构建这个核心模块：

> "请在 `schema_linking` 目录下新建 `iterative_expander.py`。
> **任务要求：**
> 实现一个 `IterativeSubgraphExpander` 类，它接收一个由 NetworkX 管理的数据库 Schema 图对象，以及一个初始表列表 (List[str])。
> **核心方法 `run_expansion(question, initial_tables)`：**
> 1. 实现一个 `while` 循环（最大迭代次数设为 3）。
> 2. **计算上下文**：
> * 使用 NetworkX 找出当前 `core_tables` 之间的内部关联外键（Mandatory Keys）。
> * 找出当前 `core_tables` 的所有 1-hop 邻居表 (Frontier tables)。
> 
> 
> 3. **Prompt 组装**：构建一个包含核心表及其列（带 samples 统计特征）、1-hop 邻居表简介、以及上一轮推理理由的 Prompt。
> 4. **LLM 调用与解析**：要求 LLM 返回 JSON 格式，包含 `selected_columns_from_core`, `selected_tables_from_frontier`, 和 `to_solve_the_question (is_solvable, reasoning)`。
> 5. **循环控制**：
> * 如果 `is_solvable` 为 true，终止循环。
> * 如果为 false，将 `selected_tables_from_frontier` 加入 `core_tables`，记录 reasoning，继续循环。
> 
> 
> 6. **收尾阶段 (Pruning)**：循环结束后，根据最终选定的列和表，利用 `G.subgraph()` 截取并返回最终的 NetworkX 最小子图对象。同时清理掉未选中任何业务列的边缘表。
> 
> 
> **请确保：**
> * 与图的操作强依赖 `networkx` API (如 `neighbors`, `subgraph`)。
> * 加入完善的异常处理（如 LLM 返回 JSON 解析失败时的重试机制）。"
> 
> 

---

### 💡 这个设计的卓越之处：

1. **高度容错**：“宁缺毋滥”的初始策略保证了你有一个绝对干净的起点。哪怕初始只有 1 个表，它也能像滚雪球一样，每次绝对安全地滚入 1 层相关表。
2. **防断路**：因为每次扩展都是严格从 NetworkX 计算出的 1-hop 邻居中选择，新加入的表**百分之百在物理上与大本营相连**。
3. **继承了你最喜欢的“推理感”**：保留了旧版 `sl2.py` 中出色的 `reasoning` 和 `is_solvable` 判断机制，让 LLM 自己决定何时收手，赋予了 Agent 充分的自主决策权，同时也兼顾了 Token 开销的最小化。