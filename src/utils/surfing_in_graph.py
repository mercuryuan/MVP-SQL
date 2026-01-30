import time
from typing import List

from neo4j import GraphDatabase

from src.neo4j_connector import get_driver


class Neo4jExplorer:
    def __init__(self):
        self.driver = get_driver()

    def get_all_nodes(self):
        query = """
        MATCH (n)
        RETURN labels(n) AS labels, properties(n) AS properties
        """
        with self.driver.session() as session:
            result = session.run(query)
            nodes = [record for record in result]
        return nodes

    def get_all_relationships(self):
        query = """
        MATCH ()-[r]->()
        RETURN type(r) AS type, properties(r) AS properties
        """
        with self.driver.session() as session:
            result = session.run(query)
            relationships = [record for record in result]
        return relationships

    def get_all_tables(self):
        query = """
        MATCH (t:Table)
        RETURN t.name AS name, properties(t) AS properties
        """
        with self.driver.session() as session:
            result = session.run(query)
            tables = {record["name"]: record["properties"] for record in result}
        return tables

    def get_all_columns(self):
        """
        获取所有列的信息
        :return:
        """
        query = """
        MATCH (c:Column)
        RETURN properties(c) AS properties
        """
        with self.driver.session() as session:
            result = session.run(query)
            columns = [record["properties"] for record in result]
        return columns

    def get_all_foreign_keys(self):
        query = """
        MATCH ()-[r:FOREIGN_KEY]->()
        RETURN properties(r) AS properties
        """
        with self.driver.session() as session:
            result = session.run(query)
            foreign_keys = [record["properties"] for record in result]
        return foreign_keys

    import time

    def get_columns_for_table(self, table_name, max_retries=5, retry_delay=1):
        """
        获取指定表的列信息，若失败则最多重试 max_retries 次，仍失败则抛出异常。

        参数：
            table_name (str): 表名
            max_retries (int): 最大重试次数
            retry_delay (int): 每次重试之间的等待时间（秒）

        返回：
            dict: 列名到属性的映射
        """
        query = """
        MATCH (t:Table {name: $table_name})-[:HAS_COLUMN]->(c:Column)
        RETURN c.name AS name, properties(c) AS properties
        """

        for attempt in range(1, max_retries + 1):
            with self.driver.session() as session:
                result = session.run(query, table_name=table_name)
                columns = {record["name"]: record["properties"] for record in result}

            if columns:
                return columns
            else:
                print(f"[WARNING] 第 {attempt} 次尝试：没有找到表 '{table_name}' 的列信息。")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(f"[ERROR] 重试 {max_retries} 次后仍未能获取表 '{table_name}' 的列信息，程序终止。")

        # 理论上永远到不了这里
        return {}

    def get_neighbor_tables(self, table_name, n_hop):
        query = """
        MATCH (t:Table {name: $table_name})
        CALL apoc.path.expandConfig(t, {relationshipFilter: "FOREIGN_KEY", minLevel: $n_hop, maxLevel: $n_hop, labelFilter: "+Table"}) 
        YIELD path
        RETURN DISTINCT last(nodes(path)).name AS neighbor_table
        """
        with self.driver.session() as session:
            result = session.run(query, table_name=table_name, n_hop=n_hop)
            neighbor_tables = [record["neighbor_table"] for record in result]

        if not neighbor_tables:
            print(f"[WARNING] 没有找到 {n_hop}-hop 的相关表，请检查表名或数据库连接。")

        return neighbor_tables

    def is_subgraph_connected(self, selected_tables):
        if not selected_tables:
            return False

        visited = set()
        queue = [selected_tables[0]]  # 从任意一个表开始 BFS

        while queue:
            current_table = queue.pop(0)
            if current_table in visited:
                continue

            visited.add(current_table)
            neighbors = self.get_neighbor_tables(current_table, 1)
            for neighbor in neighbors:
                if neighbor in selected_tables and neighbor not in visited:
                    queue.append(neighbor)

        return len(visited) == len(selected_tables)

    def bfs_subgraph(self, selected_tables):
        all_tables = set(self.get_all_tables().keys())
        invalid_tables = [table for table in selected_tables if table not in all_tables]

        if invalid_tables:
            raise (f"[ERROR] 以下表不存在于数据库中: {invalid_tables}")
            return []

        if not self.is_subgraph_connected(selected_tables):
            print("[ERROR] 选定的子图不是连通的，请检查输入的表。")
            return []

        visited = set(selected_tables)  # 已选择的子图中的表都算已访问
        queue = [(table, 0) for table in selected_tables]  # 初始化队列
        result = []

        while queue:
            level_size = len(queue)
            level_tables = []

            for _ in range(level_size):
                current_table, level = queue.pop(0)

                # 记录当前层的表
                level_tables.append(current_table)

                # 获取 1-hop 邻居
                neighbors = self.get_neighbor_tables(current_table, 1)
                for neighbor in neighbors:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, level + 1))

            if level_tables:
                result.append(level_tables)

        unvisited_tables = all_tables - visited
        if unvisited_tables:
            print(f"[DEBUG] 存在未被访问的表: {unvisited_tables}")

        return result

    def get_foreign_keys_between_tables(self, table1, table2):
        """
        获取两个表之间的直接外键关系。

        该函数通过执行 Neo4j 的 Cypher 查询来查找两个指定表之间的外键关系，
        返回所有匹配的外键引用路径。如果查询结果为空，则打印提示信息表示两个表之间
        没有直接的外键关系。

        参数:
            table1 (str): 第一个表的名称。
            table2 (str): 第二个表的名称。

        返回:
            list: 包含外键引用路径的字符串列表。如果不存在外键关系，则返回空列表。
        """
        query = """
        MATCH (t1:Table {name: $table1})-[r:FOREIGN_KEY]-(t2:Table {name: $table2})
        RETURN r.reference_path AS reference_path
        """
        with self.driver.session() as session:
            result = session.run(query, table1=table1, table2=table2)
            foreign_keys = [record["reference_path"] for record in result if record["reference_path"]]

        # if not foreign_keys:
        #     print(f"[INFO] 表 '{table1}' 和表 '{table2}' 之间没有直接的外键关系。")

        return foreign_keys


class SchemaGenerator:
    def __init__(self):
        """
        初始化 SchemaGenerator，并从 Neo4jExplorer 获取所有表信息。
        """
        self.explorer = Neo4jExplorer()
        self.tables = self.explorer.get_all_tables()

    numeric_types = [
        "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
        "REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BOOLEAN"
    ]

    text_types = [
        "TEXT", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR", "NTEXT",
        "CLOB", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "JSON", "XML"
    ]

    datetime_types = ["DATE", "DATETIME", "TIMESTAMP"]

    def generate_table_description(self, table_name, mode="full", selected_tables=None):
        """
        根据提供的表信息，智能生成结构化文字描述。
        该方法根据指定的表名和模式，生成包含表结构、列信息、主键、外键、行数和列数等详细信息的描述。
        支持三种模式："full"（完整）, "brief"（简略）, "minimal"（极简）。
            完整模式（"full"）：包含所有列的详细信息，包括数据类型、列描述、示例值等。
            简略模式（"brief"）：仅包含列名和数据类型，适用于快速概览。
            极简模式（"minimal"）：只包含表名和列名，适用于快速查找。
        如果提供了 selected_tables 参数，则根据提供的表列表生成外键引用路径。（控制LLM进行子图漫游的1-hop视角）
        如果未提供 selected_tables 参数，则根据表的 referenced_by 和 reference_to 属性生成外键引用路径。


        :param table_name: 需要生成描述的表名
        :param mode: "full"（完整）, "brief"（简略）, "minimal"（极简）
        :param selected_tables: 可选，用于生成外键引用路径的表列表
        :return: 结构化文字描述
        """
        table_info = self.tables.get(table_name, {})
        if not table_info:
            return f"# Table: {table_name} (No information available)"
        table_name = table_info.get("name", "未知表名")
        columns = table_info.get("columns", [])
        primary_key = table_info.get("primary_key")
        foreign_keys = table_info.get("foreign_key")
        row_count = table_info.get("row_count")
        column_count = table_info.get("column_count", len(columns))
        description = table_info.get("description")
        #
        reference_paths = []
        if selected_tables:
            for table in selected_tables:
                reference_path = self.explorer.get_foreign_keys_between_tables(table_name, table)
                reference_paths += reference_path
        else:
            referenced_by = table_info.get("referenced_by", [])
            reference_to = table_info.get("reference_to", [])
            reference_paths = reference_to + referenced_by

        description_lines = [f"# Table: {table_name}", "["]

        if mode == "full":
            # if column_count:
            #     description_lines.append(f"Column Count: {column_count}")
            # if primary_key:
            #     description_lines.append(f"Primary Key: {primary_key}")

            # if foreign_keys:
            #     if isinstance(foreign_keys, list):
            #         foreign_key_str = ", ".join(foreign_keys)
            #     else:
            #         foreign_key_str = str(foreign_keys)
            #     description_lines.append(f"Foreign Key: {foreign_key_str}")
            if description:
                description_lines.append(f"Description: {description}")
            if columns:
                description_lines.append(f"Columns: {', '.join(columns)}")
            if row_count:
                description_lines.append(f"Row Count: {row_count}")

            # if referenced_by:
            #     # description_lines.append(f"Referenced By: {', '.join(referenced_by)}")
            #     description_lines.append(f"Referenced By: {referenced_by}")
            #
            # if reference_to:
            #     # description_lines.append(f"Reference To: {', '.join(reference_to)}")
            #     description_lines.append(f"Reference To: {reference_to}")
            if reference_paths:
                # description_lines.append(f"Reference To: {', '.join(reference_to)}")
                description_lines.append(f"Reference Path: {reference_paths}")

        elif mode == "brief":  # 有待开发
            description_lines = [
                f"# Table: {table_name}",
                f"Columns: {', '.join(columns)}",
                f"Primary Key: {primary_key}" if primary_key else "",
                f"Row Count: {row_count}" if row_count else "",
            ]

        elif mode == "minimal":
            description_lines = [f"# Table: {table_name}", f"Column: {columns}"]

        return "\n".join(filter(None, description_lines))  # 过滤掉空字符串

    def generate_column_description(self, column_info, mode="full"):
        """
        根据 column_info 动态构造输出描述，只有存在的属性才会输出。

        输出顺序：
          1. 基本属性: name, data_type, column_description, samples
          2. 完整性属性: null_count, data_integrity, sample_count, is_nullable
             （其中仅在 is_nullable 为 True 时输出 data_integrity 与 null_count，且 null_count 仅当非0时输出）
          3. 键类型属性: key_type（格式化为 Primary Key / Foreign Key）
          4. 语义属性: value_description
          5. 数值型属性（仅针对 numeric 类型）：numeric_range, numeric_mean, numeric_mode
          6. 文本型属性（仅针对 text 类型）：text_categories, average_char_length, word_frequency
          7. 时间型属性（仅针对 datetime 类型）：earliest_time, latest_time, time_span

        三种模式：
          - full：输出所有存在的属性
          - brief：只输出基本属性（name, data_type, column_description, samples）、键类型和部分完整性信息（如 data_integrity）
          - minimal：仅输出 name 和 data_type
        """
        # mode字符串的限定
        if mode not in ["full", "brief", "minimal"]:
            raise ValueError("mode 参数必须是 'full', 'brief' 或 'minimal'")
        # 基本属性
        name = column_info.get("name", "未知列")
        data_type = column_info.get("data_type", "未知类型")
        base_data_type = data_type.split("(")[0].upper()
        column_description = column_info.get("column_description", None)
        samples = column_info.get("samples", [])
        # 限制最多6个样本
        samples = samples[:6] if samples else None
        samples_str = f"Examples: [{', '.join(map(str, samples))}]" if samples else None

        # 完整性属性（仅在 full 模式下完整展示）
        is_nullable = column_info.get("is_nullable", None)
        if is_nullable is not None:
            nullable_str = "Nullable" if is_nullable else "Not Nullable"
        else:
            nullable_str = None

        # 根据参考逻辑，仅在 is_nullable 为 True 时输出 data_integrity 与 null_count（null_count 非0时）
        integrity_info = []
        if is_nullable:
            data_integrity = column_info.get("data_integrity")
            null_count = column_info.get("null_count")
            if data_integrity is not None:
                integrity_info.append(f"DataIntegrity: {data_integrity}")
                if null_count and null_count != 0:
                    integrity_info.append(f"NullCount: {null_count}")

        # 键类型属性
        key_type = column_info.get("key_type", [])
        key_info = []
        if "primary_key" in key_type:
            key_info.append("Primary Key")
        if "foreign_key" in key_type:
            key_info.append("Foreign Key")
        key_info_str = ", ".join(key_info) if key_info else None

        # 语义属性
        value_description = column_info.get("value_description", None)
        value_description_str = f"ValueDescription: {value_description}" if value_description else None

        # 构造描述列表，只有属性存在时才添加
        details = [f"({name}:{base_data_type}"]

        if mode == "minimal":
            # 仅基本信息
            return ",".join(details) + ")"

        # brief 模式：基本属性、键类型、样本、以及简略的完整性信息（只输出 data_integrity）
        if mode == "brief":
            if column_description:
                details.append(column_description)
            if key_info_str:
                details.append(key_info_str)
            if samples_str:
                details.append(samples_str)
            if nullable_str:
                details.append(nullable_str)
            return ",".join(details) + ")"

        # full 模式：输出所有存在的属性
        if column_description:
            details.append(column_description)
        if key_info_str:
            details.append(key_info_str)
        if samples_str:
            details.append(samples_str)
        if nullable_str:
            details.append(nullable_str)
        if integrity_info:
            details.extend(integrity_info)
        # # 如果有 sample_count（完整性扩展）也可输出
        # sample_count = column_info.get("sample_count", None)
        # if sample_count is not None:
        #     details.append(f"SampleCount: {sample_count}")
        # if value_description_str:
        #     details.append(value_description_str)

        # 数值型特有属性
        if base_data_type in self.numeric_types:
            if "numeric_range" in column_info:
                details.append(f"Range: {column_info['numeric_range']}")
            if "numeric_mean" in column_info:
                details.append(f"NumericMean: {column_info['numeric_mean']}")
            if "numeric_mode" in column_info:
                details.append(f"NumericMode: {column_info['numeric_mode']}")

        # 文本型特有属性
        if base_data_type in self.text_types:
            if "text_categories" in column_info:
                details.append(f"TextCategories: {column_info['text_categories']}")
            if "average_char_length" in column_info:
                details.append(f"AverageCharLength: {column_info['average_char_length']}")
            if "word_frequency" in column_info:
                details.append(f"WordFrequency: {column_info['word_frequency']}")

        # 时间型特有属性
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
        根据提供的表名，生成所有表的结构化描述。
        :param table_names: 需要生成描述的表名列表
        :return: 所有表的结构化描述
        """
        descriptions = []
        # 生成表的描述
        if selected_tables:
            table_description = self.generate_table_description(table_name, selected_tables=selected_tables)
        else:
            table_description = self.generate_table_description(table_name)
        descriptions.append(table_description)
        # 生成列的描述
        columns = self.explorer.get_columns_for_table(table_name)
        for col in columns.keys():
            column_description = self.generate_column_description(columns[col], mode=detail_level)
            descriptions.append(column_description)
        return "\n".join(descriptions) + "\n]"


if __name__ == "__main__":
    import logging

    logging.getLogger("neo4j").setLevel(logging.ERROR)  # 屏蔽警告

    explorer = Neo4jExplorer()

    try:
        # for hop in explorer.bfs("Faculty"):
        #     print(hop)
        # print()
        # for hop in explorer.bfs_subgraph(["lake"]):
        #     print(hop)
        sg = SchemaGenerator()
        # print(explorer.get_foreign_keys_between_tables("book", "book_author"))
        tables = explorer.get_all_tables()
        # print(tables)
        for table in tables:
            print()
            # print(sg.generate_table_description(table, selected_tables=["book"]))
            print(sg.generate_combined_description(table, selected_tables=["customer_address", "address"]))

        # print(sg.generate_table_description(tables[1]))
        # print(sg.generate_table_description(tables[12], "minimal"))

        # col = explorer.get_columns_for_table("League")
        # for record in col:
        #     # print(record)
        #     print(sg.generate_column_description(col[record]))
        # print()
        # for record in col:
        #     # print(record)
        #     print(sg.generate_column_description(record, "brief"))
        # print()
        # for record in col:
        #     # print(record)
        #     print(sg.generate_column_description(record, "minimal"))
    finally:
        explorer.driver.close()
