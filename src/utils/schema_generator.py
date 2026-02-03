import os
from ast import main
from typing import List, Dict, Any
import logging
import networkx as nx
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import configs.paths
# 使用基于项目根目录的完整路径
from src.utils.graph_explorer import GraphExplorer
from src.utils.graph_loader import GraphLoader
# --- 绝对导入结束 ---

logger = logging.getLogger(__name__)


class SchemaGenerator:
    """
    SchemaGenerator 类
    ----------------
    功能: 负责解析 NetworkX 图结构（代表数据库Schema），并生成结构化的文本描述。
    用途: 生成的文本通常作为 Prompt 提供给 LLM，帮助其理解数据库结构、字段含义及数据分布。
    """

    def __init__(self, graph: nx.DiGraph):
        """
        初始化 SchemaGenerator。

        Args:
            graph (nx.DiGraph): 包含数据库元数据的 NetworkX 有向图对象。
                                节点通常代表表或列，边代表归属关系或外键关系。
        """
        # 初始化图遍历器，用于封装复杂的图查询操作
        self.explorer = GraphExplorer(graph)
        # 预先获取并缓存所有表节点的信息，提高后续查询效率
        self.tables = self.explorer.get_all_tables()

    # ================= 定义数据类型常量 =================
    # 用于后续根据列的数据类型，决定在描述中展示哪些特定的统计信息

    # 数值类型：可能包含范围、均值、众数等统计信息
    numeric_types = [
        "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
        "REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BOOLEAN"
    ]

    # 文本类型：可能包含分类（Categories）、平均长度、词频等信息
    text_types = [
        "TEXT", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR", "NTEXT",
        "CLOB", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "JSON", "XML"
    ]

    # 时间类型：可能包含最早时间、最晚时间、时间跨度等信息
    datetime_types = ["DATE", "DATETIME", "TIMESTAMP"]

    def generate_table_description(self, table_name, mode="full", selected_tables=None):
        """
        生成表的结构化文本描述。

        Args:
            table_name (str): 目标表名。
            mode (str): 描述的详细程度。
                        - 'full': 包含描述、列名、行数、外键引用路径。
                        - 'brief': 仅包含列名、主键、行数。
                        - 'minimal': 仅包含列名列表。
            selected_tables (List[str], optional): 当前上下文中选定的其他表。
                                                   如果有值，则只计算与这些表之间的外键路径。

        Returns:
            str: 格式化后的表描述字符串。
        """
        # 获取表的基础信息，如果不存在则返回默认提示
        table_info = self.tables.get(table_name, {})
        if not table_info:
            return f"# Table: {table_name} (No information available)"

        # 提取表元数据
        real_table_name = table_info.get("name", "Unknown Table")
        columns = table_info.get("columns", [])
        primary_key = table_info.get("primary_key")
        # foreign_keys = table_info.get("foreign_key") # 暂时未直接使用，依靠 reference_paths
        row_count = table_info.get("row_count")
        # 列数：优先取 explicitly stored 的 count，否则计算列表长度
        column_count = table_info.get("column_count", len(columns))
        description = table_info.get("description")

        # --- 计算引用路径 (Foreign Key Paths) ---
        reference_paths = []
        if selected_tables:
            # 如果指定了选定表集合，只查找当前表与选定表之间的外键关系
            for table in selected_tables:
                # 获取两表之间的外键路径
                paths = self.explorer.get_foreign_keys_between_tables(real_table_name, table)
                reference_paths += paths
        else:
            # 默认为空时，列出该表所有 "被引用" 和 "引用别人" 的关系
            referenced_by = table_info.get("referenced_by", [])
            reference_to = table_info.get("reference_to", [])
            reference_paths = reference_to + referenced_by

        # --- 构建描述文本 ---
        description_lines = [f"# Table: {real_table_name}", "["]

        if mode == "full":
            if description:
                description_lines.append(f"Description: {description}")
            if columns:
                # 拼接所有列名
                description_lines.append(f"Columns: {', '.join(columns)}")
            if row_count:
                description_lines.append(f"Row Count: {row_count}")
            if reference_paths:
                # 能够帮助模型理解表之间的连接（JOIN）路径
                description_lines.append(f"Reference Path: {reference_paths}")

        elif mode == "brief":
            description_lines = [
                f"# Table: {real_table_name}",
                f"Columns: {', '.join(columns)}",
                f"Primary Key: {primary_key}" if primary_key else "",
                f"Row Count: {row_count}" if row_count else "",
            ]

        elif mode == "minimal":
            description_lines = [f"# Table: {real_table_name}", f"Column: {columns}"]

        # 过滤掉空行并用换行符连接
        return "\n".join(filter(None, description_lines))

    def generate_column_description(self, column_info, mode="full"):
        """
        生成列（字段）的结构化描述。

        Args:
            column_info (dict): 包含列属性的字典（来自 Graph 节点）。
            mode (str): 详细模式 ('full', 'brief', 'minimal')。

        Returns:
            str: 格式如 "(name:type, ...)" 的列描述字符串。
        """
        if mode not in ["full", "brief", "minimal"]:
            raise ValueError("mode parameter must be 'full', 'brief' or 'minimal'")

        # 1. 提取基础信息
        name = column_info.get("name", "Unknown Column")
        data_type = column_info.get("data_type", "Unknown Type")
        # 获取基础类型（移除长度限制，如 VARCHAR(255) -> VARCHAR）
        base_data_type = data_type.split("(")[0].upper()

        column_description = column_info.get("column_description", None)

        # 2. 处理样本数据 (Samples) - 限制最多显示6个
        samples = column_info.get("samples", [])
        samples = samples[:6] if samples else None
        samples_str = f"Examples: [{', '.join(map(str, samples))}]" if samples else None

        # 3. 处理空值信息
        is_nullable = column_info.get("is_nullable", None)
        nullable_str = "Nullable" if is_nullable else "Not Nullable" if is_nullable is not None else None

        # 4. 数据完整性信息 (Data Integrity)
        integrity_info = []
        if is_nullable:
            data_integrity = column_info.get("data_integrity")
            null_count = column_info.get("null_count")
            if data_integrity is not None:
                integrity_info.append(f"DataIntegrity: {data_integrity}")
                if null_count and null_count != 0:
                    integrity_info.append(f"NullCount: {null_count}")

        # 5. 主键/外键类型判断 (Key Type)
        # 逻辑：优先查看 'key_type' 列表，如果不存在则检查 'is_primary_key' 等布尔标志
        key_info = []
        key_type = column_info.get("key_type")
        if key_type:
            if "primary_key" in key_type: key_info.append("Primary Key")
            if "foreign_key" in key_type: key_info.append("Foreign Key")
        else:
            if column_info.get("is_primary_key"): key_info.append("Primary Key")
            if column_info.get("is_foreign_key"): key_info.append("Foreign Key")

        key_info_str = ", ".join(key_info) if key_info else None

        value_description = column_info.get("value_description", None)
        # value_description_str = f"ValueDescription: {value_description}" if value_description else None

        # --- 开始构建字符串 ---
        # 格式开始：(列名:类型
        details = [f"({name}:{base_data_type}"]

        # 模式：Minimal (最简模式)
        if mode == "minimal":
            return ",".join(details) + ")"

        # 模式：Brief (简介模式) - 包含描述、键类型、样本、可空性
        if mode == "brief":
            if column_description: details.append(column_description)
            if key_info_str: details.append(key_info_str)
            if samples_str: details.append(samples_str)
            if nullable_str: details.append(nullable_str)
            return ",".join(details) + ")"

        # 模式：Full (全模式) - 添加更多统计信息
        if column_description: details.append(column_description)
        if key_info_str: details.append(key_info_str)
        if samples_str: details.append(samples_str)
        if nullable_str: details.append(nullable_str)
        if integrity_info: details.extend(integrity_info)
        # if value_description_str: details.append(value_description_str)

        # 6. 根据数据类型追加特定的统计特征 (Data Distribution Stats)

        # 处理数值类型 (Range, Mean, Mode)
        if base_data_type in self.numeric_types:
            if "numeric_range" in column_info:
                details.append(f"Range: {column_info['numeric_range']}")
            if "numeric_mean" in column_info:
                details.append(f"NumericMean: {column_info['numeric_mean']}")
            if "numeric_mode" in column_info:
                details.append(f"NumericMode: {column_info['numeric_mode']}")

        # 处理文本类型 (Categories, Length, Word Frequency)
        if base_data_type in self.text_types:
            if "text_categories" in column_info:
                details.append(f"TextCategories: {column_info['text_categories']}")
            if "average_char_length" in column_info:
                details.append(f"AverageCharLength: {column_info['average_char_length']}")
            if "word_frequency" in column_info:
                details.append(f"WordFrequency: {column_info['word_frequency']}")

        # 处理时间类型 (Earliest, Latest, Span)
        if base_data_type in self.datetime_types:
            if "earliest_time" in column_info:
                details.append(f"EarliestTime: {column_info['earliest_time']}")
            if "latest_time" in column_info:
                details.append(f"LatestTime: {column_info['latest_time']}")
            if "time_span" in column_info:
                details.append(f"TimeSpan: {column_info['time_span']}")

        return ",".join(details) + ")"

    def generate_combined_description(self, table_name, detail_level="full", selected_tables: List[str] = None):
        """
        生成包含表信息及其所有列信息的完整描述块。

        格式示例:
        # Table: Users
        [
        Description: 用户表
        Columns: id, name, age
        (id:INT, Primary Key, Examples:[1,2])
        (name:VARCHAR, Examples:[Alice, Bob])
        ...
        ]

        Args:
            table_name (str): 表名。
            detail_level (str): 详细程度 ('full', 'brief', 'minimal').
            selected_tables (List[str]): 用于上下文的外键路径计算。

        Returns:
            str: 组合好的多行描述字符串。
        """
        descriptions = []

        # 1. 生成表头描述 (包含表级元数据)
        if selected_tables:
            table_description = self.generate_table_description(table_name, selected_tables=selected_tables)
        else:
            table_description = self.generate_table_description(table_name)
        descriptions.append(table_description)

        # 2. 获取该表的所有列节点
        columns = self.explorer.get_columns_for_table(table_name)

        # 3. 循环生成每一列的描述
        for col in columns.keys():
            column_description = self.generate_column_description(columns[col], mode=detail_level)
            descriptions.append(column_description)

        # 4. 闭合描述块
        return "\n".join(descriptions) + "\n]"


# ================= 测试入口 =================
if __name__ == "__main__":
    # 1. 配置日志格式：时间 - 级别 - 消息
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # 2. 定义图文件路径 (注意：请根据实际环境修改此路径)
    pkl_path = os.path.join(configs.paths.OUTPUT_ROOT, r"schema_graph_repo\bird\books\books.pkl")

    logger.info(f"正在加载图文件: {pkl_path}")

    # 3. 加载图结构
    # GraphLoader 是外部工具类，用于从 .pkl 文件反序列化 NetworkX 图
    G = GraphLoader.load_graph(pkl_path)

    if G is None:
        logger.error("图结构加载失败，文件可能不存在或损坏，程序退出。")
        exit(1)

    # 4. 打印图的基本统计信息
    logger.info(f"加载成功 -> 节点数: {G.number_of_nodes()}, 边数: {G.number_of_edges()}")

    # 5. 实例化 Schema 生成器
    sg = SchemaGenerator(G)

    logger.info("开始生成 Schema 描述...\n" + "=" * 50)

    # 6. 遍历并打印数据库中所有表的详细描述
    # 这里的 sg.tables 是一个字典，key 是 table_name
    for table_name in sg.tables:
        print(f"正在处理表: {table_name}")
        description = sg.generate_combined_description(table_name, detail_level="full")
        print(description)
        print("-" * 30)  # 分隔线
