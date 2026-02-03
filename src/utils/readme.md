
## 📄 文档二：`DataLoader` 工具类技术文档

### 1. 类概述

`DataLoader` 是一个科研级通用数据加载工具，专门用于标准化 Text-to-SQL 任务的数据预处理流程。它支持按需过滤字段、跨数据集映射以及针对不同数据集特性的自动补全。

### 2. 核心功能

* **多源加载**：一键切换 `spider` (合并 train+other), `spider_dev`, `bird`, `bird_dev`。
* **字段规范化 (Normalization)**：通过 `COLUMN_MAPPING` 解决跨数据集字段名不一致的问题。
* **自动对齐 (Alignment)**：当请求字段在当前数据集缺失时（如 Spider 的 `evidence`），自动补全为 `None`。
* **数据清洗**：统一 SQL 字符串格式，去除前后空格及末尾分号。
* **数据库筛选**：支持通过 `db_id` 进行切片，方便针对特定数据库进行 Schema Linking 推理。

### 3. 关键方法说明

#### `__init__(self, dataset_name)`

* **功能**：初始化加载器并根据配置路径读取 JSON 文件。
* **特殊逻辑**：当传入 `spider` 时，会自动合并官方的 `train_spider.json` 和 `train_others.json` 以获取完整训练集。

#### `filter_data(self, db_id=None, fields=None, show_count=False)`

* **参数**：
* `db_id` (str, 可选): 指定数据库 ID 过滤数据。
* `fields` (List[str], 可选): 统一后的目标字段名列表，如 `["question", "sql_query", "evidence"]`。
* `show_count` (bool): 是否打印处理进度。


* **返回**：`List[Dict]` 处理后的数据列表。

#### `list_dbnames(self)`

* **功能**：遍历当前数据集，返回所有唯一的数据库 ID 列表，常用于调试和批量推理脚本。

### 4. 字段映射表 (Mapped Schemas)

工具类对外暴露的统一字段名及其原始对应关系：

| 统一后的 Key | 对应 Spider 原始 Key | 对应 BIRD 原始 Key | 说明 |
| --- | --- | --- | --- |
| **`sql_query`** | `query` | `SQL` | 提取为字符串，已剥离分号 |
| **`question`** | `question` | `question` | 自然语言问题 |
| **`evidence`** | (自动补全 `None`) | `evidence` | 外部知识说明 |
| **`db_id`** | `db_id` | `db_id` | 数据库标识符 |

### 5. 使用示例

```python
loader = DataLoader("spider_dev")
# 即便 Spider 没 evidence，这样写也不会报错，会返回 None
data = loader.filter_data(fields=["question", "sql_query", "evidence"])
print(data[0]) 
# 输出: {'question': '...', 'sql_query': '...', 'evidence': None}

```

---

**接下来，我还可以为你生成一份针对这两个数据集的 `README.md` 安装与路径配置指南，或者帮你写一段利用此工具进行批量 Prompt 构造的代码示例。你需要哪一个？**