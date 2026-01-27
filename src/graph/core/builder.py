import networkx as nx
from .utils import generate_fk_hash


class GraphBuilder:
    def __init__(self):
        """
        初始化 NetworkX 图构建器。
        使用 DiGraph (有向图) 存储 Schema 结构。
        """
        self.G = nx.DiGraph()

    def add_table_node(self, table_name, **properties):
        """
        创建表节点。

        :param table_name: 表名 (作为在 NetworkX 中的唯一 Key)
        :param properties: 表的其他属性 (row_count, primary_key 等)
        """
        # 在 NetworkX 中，add_node 如果节点已存在会更新属性，不存在则创建
        # 我们预先初始化列表属性，确保后续 append 操作安全
        base_props = {
            "type": "Table",
            "name": table_name,
            # 预初始化引用列表，替代 Neo4j 的 coalesce 操作
            "reference_to": [],
            "referenced_by": []
        }
        # 合并传入的属性 (properties 覆盖 base_props)
        final_props = {**base_props, **properties}

        self.G.add_node(table_name, **final_props)

    def add_column_node(self, table_name, column_name, is_primary_key, is_foreign_key, **properties):
        """
        创建列节点，并自动创建从表到列的 'HAS_COLUMN' 边。

        :param table_name: 所属表名
        :param column_name: 列名
        """
        # 1. 生成列节点的唯一 Key (格式: "Table.Column")
        col_node_id = f"{table_name}.{column_name}"

        # 2. 准备属性
        base_props = {
            "type": "Column",
            "name": column_name,
            "belongs_to": table_name,  # 保留所属关系属性方便反查
            "referenced_to": [],  # 预初始化
            "referenced_by": [],
            # --- 【修正】显式将主外键状态写入节点属性 ---
            "is_primary_key": is_primary_key,
            "is_foreign_key": is_foreign_key
        }

        # 3. 合并其他属性
        # 注意：如果 properties (即 pipeline 传来的 final_props) 中也包含同名key，
        # 这里的合并顺序 {**base, **prop} 会让 properties 覆盖 base_props。
        # 由于 pipeline 中没有传这两个值进 final_props，所以这里是安全的。
        final_props = {**base_props, **properties}

        # 4. 添加列节点
        self.G.add_node(col_node_id, **final_props)

        # 5. 确定关系类型 (完全保留原逻辑用于区分 PK/FK)
        if is_primary_key and is_foreign_key:
            relation_type = "primary_and_foreign_key"
        elif is_primary_key:
            relation_type = "primary_key"
        elif is_foreign_key:
            relation_type = "foreign_key"
        else:
            relation_type = "normal_column"

        # 6. 添加边: Table -> Column
        # 注意：NetworkX 的边属性直接作为参数传递
        self.G.add_edge(table_name, col_node_id,
                        type="HAS_COLUMN",
                        relation_type=relation_type)

    def add_foreign_key(self, from_table, from_column, to_table, to_column):
        """
        处理外键逻辑：
        1. 创建 Table -> Table 的边
        2. 更新 4 个相关节点的引用列表 (referenced_by / reference_to)
        """
        # 构造辅助 ID
        reference_path = f"{from_table}.{from_column}={to_table}.{to_column}"
        fk_hash = generate_fk_hash(from_table, from_column, to_table, to_column)

        # 1. 添加 Table -> Table 的边
        self.G.add_edge(from_table, to_table,
                        type="FOREIGN_KEY",
                        from_table=from_table,
                        from_column=from_column,
                        to_table=to_table,
                        to_column=to_column,
                        reference_path=reference_path,
                        fk_hash=fk_hash)

        # 2. 更新节点属性 (Python 内存操作比 Cypher 简单得多)
        # 我们直接获取节点对象引用 (字典)，然后操作列表

        # Helper：安全追加列表的闭包函数
        def safe_append(node_id, prop_key, value):
            if self.G.has_node(node_id):
                # 直接访问字典，比 set_node_attributes 更快
                node_attrs = self.G.nodes[node_id]
                # 确保列表存在 (虽然 add_node 时已初始化，但防守式编程不亏)
                if prop_key not in node_attrs:
                    node_attrs[prop_key] = []
                node_attrs[prop_key].append(value)

        # A. 更新目标表 (被谁引用了)
        safe_append(to_table, "referenced_by", reference_path)

        # B. 更新源表 (引用了谁)
        safe_append(from_table, "reference_to", reference_path)

        # C. 更新源列 (引用了谁)
        from_col_id = f"{from_table}.{from_column}"
        safe_append(from_col_id, "referenced_to", f"{to_table}.{to_column}")

        # D. 更新目标列 (被谁引用了)
        to_col_id = f"{to_table}.{to_column}"
        safe_append(to_col_id, "referenced_by", f"{from_table}.{from_column}")

    def save_graph(self, output_path):
        """
        将图保存到磁盘。
        推荐使用 pickle (Python 原生) 或 gexf/graphml (通用格式)。
        这里默认用 pickle，因为它能完美保留 Python 对象类型 (如列表、None)。
        """
        import pickle
        # 确保目录存在
        import os
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        with open(output_path, 'wb') as f:
            pickle.dump(self.G, f)
        print(f"Graph successfully saved to {output_path}")

    def get_graph(self):
        return self.G
