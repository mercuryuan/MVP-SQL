import os
import sys
import pickle
from typing import Dict, List
import networkx as nx

# 将项目根目录添加到 sys.path 以便导入 configs
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from configs import paths


class GraphSchemaExtractor:
    """
    基于 NetworkX 图结构的模式提取器。
    直接从项目生成的图结构文件(.pkl)中读取数据库 Schema，
    替代直接读取 SQLite 文件的传统方式，更轻量且无需访问原始数据库。
    """

    def __init__(self, dataset_name: str):
        """
        初始化 GraphSchemaExtractor。
        :param dataset_name: 数据集名称 (e.g., 'bird', 'spider')，用于定位图文件路径。
        """
        self.dataset_name = dataset_name
        # 图文件存储根目录: output/schema_graph_repo/
        self.graph_repo_path = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo", dataset_name)

    def extract_schema(self, db_name: str) -> Dict[str, List[str]]:
        """
        从图结构文件中提取数据库模式。
        :param db_name: 数据库名称。
        :return: 数据库模式字典 {table_name: [column_names...]}.
        """
        # 构造图文件路径: output/schema_graph_repo/[dataset]/[db_name]/[db_name].pkl
        pkl_path = os.path.join(self.graph_repo_path, db_name, f"{db_name}.pkl")

        if not os.path.exists(pkl_path):
            raise FileNotFoundError(f"Graph file not found: {pkl_path}")

        # 加载图结构
        try:
            with open(pkl_path, 'rb') as f:
                G: nx.DiGraph = pickle.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load graph from {pkl_path}: {e}")

        schema = {}

        # 遍历图中所有节点
        # 我们寻找 type='Table' 的节点，它们包含表名和列名列表
        for node, attrs in G.nodes(data=True):
            if attrs.get("type") == "Table":
                table_name = attrs.get("name")
                columns = attrs.get("columns", [])
                
                # 确保 columns 是列表
                if isinstance(columns, list):
                    schema[table_name] = columns
                else:
                    schema[table_name] = []

        return schema

    def extract_foreign_keys(self, db_name: str) -> List[Dict[str, str]]:
        """
        从图结构文件中提取外键关系。
        :param db_name: 数据库名称。
        :return: 外键列表，每个元素为字典:
                 {'from_table': str, 'from_column': str, 'to_table': str, 'to_column': str}
        """
        pkl_path = os.path.join(self.graph_repo_path, db_name, f"{db_name}.pkl")
        
        if not os.path.exists(pkl_path):
            raise FileNotFoundError(f"Graph file not found: {pkl_path}")

        try:
            with open(pkl_path, 'rb') as f:
                G: nx.DiGraph = pickle.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load graph from {pkl_path}: {e}")

        fks = []
        # 遍历图中所有边，寻找 type='FOREIGN_KEY' 的边
        for u, v, attrs in G.edges(data=True):
            if attrs.get("type") == "FOREIGN_KEY":
                fk_info = {
                    "from_table": attrs.get("from_table"),
                    "from_column": attrs.get("from_column"),
                    "to_table": attrs.get("to_table"),
                    "to_column": attrs.get("to_column")
                }
                fks.append(fk_info)
        return fks



if __name__ == "__main__":
    # 测试代码
    try:
        # 假设存在 bird/books 的图文件
        extractor = GraphSchemaExtractor("bird")
        schema = extractor.extract_schema("books")
        print(f"Schema extracted for 'books' ({len(schema)} tables):")
        for table, cols in schema.items():
            print(f"  - Table: {table}, Columns: {len(cols)}")
    except Exception as e:
        print(f"Error during test: {e}")
