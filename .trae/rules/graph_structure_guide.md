# MVP-SQL 数据库图结构指南

本文档详细说明了本项目中数据库图结构（Graph Structure）的生成逻辑、组成元素及数据处理策略。该图结构基于 NetworkX DiGraph 构建，旨在为下游任务（如 Text-to-SQL、GNN 分析）提供富含语义和统计信息的结构化输入。

## 1. 生成逻辑概述

图结构的生成由 `SchemaPipeline` (位于 `src/graph/pipeline.py`) 统一调度，核心流程如下：

1.  **表节点构建**：遍历数据库所有表，提取行数、主键、外键等元数据，创建 Table 节点。
2.  **列节点构建与画像**：
    -   遍历每张表的列，读取数据（支持大表截断）。
    -   进行**数据画像 (Data Profiling)**：计算统计特征（如分布、均值、文本特征）。
    -   **元数据融合**：结合外部 CSV 描述文件（如有），增强列的语义描述。
    -   创建 Column 节点，并建立 `HAS_COLUMN` 边。
3.  **关系构建**：基于外键约束，在 Table 节点之间建立 `FOREIGN_KEY` 边，并更新相关节点的引用计数属性。
4.  **持久化**：将构建好的 NetworkX 对象序列化为 `.pkl` 文件。

---

## 2. 图结构组成

### 2.1 节点类型 (Nodes)

图中包含两种类型的节点，通过 `type` 属性区分。

#### A. 表节点 (Table Node)
-   **唯一标识 (Key)**: 表名 (e.g., `"users"`)
-   **核心属性**:
    -   `type`: `"Table"`
    -   `name`: 表名
    -   `row_count`: 总行数
    -   `column_count`: 列总数
    -   `columns`: 列名列表
    -   `primary_key`: 主键列名（或列表）
    -   `foreign_key`: 外键列名（或列表）
    -   `reference_to`: 该表引用的其他表/列的路径列表
    -   `referenced_by`: 引用该表的其他表/列的路径列表

#### B. 列节点 (Column Node)
-   **唯一标识 (Key)**: `表名.列名` (e.g., `"users.id"`)
-   **核心属性**:
    -   `type`: `"Column"`
    -   `name`: 列名
    -   `belongs_to`: 所属表名
    -   `data_type`: 原始数据类型 (e.g., `"INTEGER"`, `"VARCHAR"`)
    -   `is_primary_key`: Boolean
    -   `is_foreign_key`: Boolean
    -   `is_nullable`: Boolean
    -   **画像统计属性**: (详见下文“数据处理逻辑”)
    -   **语义属性**: `column_description`, `value_description` (来自 CSV 元数据)

### 2.2 边类型 (Edges)

#### A. 所属关系 (HAS_COLUMN)
-   **方向**: Table -> Column
-   **属性**:
    -   `type`: `"HAS_COLUMN"`
    -   `relation_type`:
        -   `"primary_key"`: 主键列
        -   `"foreign_key"`: 外键列
        -   `"primary_and_foreign_key"`: 既是主键又是外键
        -   `"normal_column"`: 普通列

#### B. 外键关系 (FOREIGN_KEY)
-   **方向**: Table (Source) -> Table (Target)
-   **属性**:
    -   `type`: `"FOREIGN_KEY"`
    -   `from_table`, `from_column`:以此表此列为起点
    -   `to_table`, `to_column`: 指向彼表彼列
    -   `reference_path`: 字符串描述 (e.g., `"t1.c1=t2.c2"`)
    -   `fk_hash`: 外键关系的哈希指纹

---

## 3. 数据处理逻辑 (Data Profiling)

`DataProfiler` 模块根据数据类型执行差异化的分析逻辑。

### 3.1 通用处理
-   **采样**: 随机抽取最多 6 个非空样本。
    -   对于文本类型，样本长度超过 30 字符会被截断。
-   **完整性**:
    -   `null_count`: 空值数量
    -   `data_integrity`: 非空值占比 (e.g., "95%")

### 3.2 数值类型 (Numeric)
*(INTEGER, REAL, DECIMAL, BOOLEAN 等)*
-   **Range**: `[min, max]`
-   **Mean**: 平均值 (Decimal 类型会转为 float 计算)
-   **Mode**: 众数 (排除 ID 类列，仅当出现次数 >1 时记录)

### 3.3 文本类型 (Text)
*(VARCHAR, TEXT, JSON 等)*
-   **Categories**: 若唯一值数量 ≤ 6，视为枚举类型，记录所有取值。
-   **Avg Length**: 平均字符长度。
-   **Word Frequency**: 词频统计 (Top 10)。
    -   特殊逻辑：频率为 1 的“孤僻词”最多只保留 3 个，且长度需 ≤ 20 字符（过滤长尾噪声）。

### 3.4 时间类型 (Time)
*(DATE, DATETIME, TIMESTAMP)*
-   **Time Span**: 时间跨度 (Max - Min)。

---

## 4. 特殊处理策略

1.  **大表处理**: 若表行数超过 100,000，仅读取前 100,000 行进行画像分析，以平衡性能与准确性。
2.  **SQLite 兼容性**:
    -   针对 SQLite 外键可能指向 `None` 的情况，自动推断指向目标表的主键。
    -   处理 SQLite 弱类型系统带来的类型推断模糊问题。
3.  **元数据增强**: 优先使用 `metadata_manager` 提供的 CSV 描述覆盖自动分析的结果，确保人工标注的高优先级。
