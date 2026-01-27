import os
import pickle
import networkx as nx
from tqdm import tqdm
from core import SQLiteHandler, DataProfiler, MetadataManager, GraphBuilder
from configs import paths


class SchemaPipeline:
    def __init__(self, database_path, output_path):
        """
        初始化 Pipeline。

        :param database_path: SQLite 数据库源文件路径
        :param output_path: 结果图存储路径 (建议以 .pkl 结尾)
        """
        self.database_path = database_path
        self.output_path = output_path

        # 初始化各个组件
        # 注意：SQLiteHandler 在 run() 中通过 with 上下文使用，此处不实例化连接
        self.profiler = DataProfiler()
        self.metadata_manager = MetadataManager(database_path)
        self.builder = GraphBuilder()

    def run(self):
        """
        执行 ETL 流程：提取 -> 分析 -> 构建图 -> 保存
        """
        print(f"Starting schema extraction for: {self.database_path}")

        # 1. 使用上下文管理器确保 SQLite 连接安全闭合
        with SQLiteHandler(self.database_path) as db:

            # --- 阶段 1: 处理表 (Table Nodes) ---
            tables = db.get_all_tables()
            print(f"Found {len(tables)} tables.")

            for table_name in tqdm(tables, desc="Processing Tables"):
                # 获取表级元数据
                row_count = db.get_row_count(table_name)
                pk_columns = db.get_primary_key_columns(table_name)
                fk_columns = db.get_foreign_key_columns(table_name)
                all_columns = [info[1] for info in db.get_columns_info(table_name)]

                # 构建表属性 (保留原逻辑)
                table_props = {
                    "row_count": row_count,
                    "column_count": len(all_columns),
                    "columns": all_columns,
                    "database_name": db.get_database_name()
                }

                # 如果有主外键，添加相应属性
                if pk_columns:
                    table_props["primary_key"] = pk_columns if len(pk_columns) > 1 else pk_columns[0]
                if fk_columns:
                    table_props["foreign_key"] = fk_columns if len(fk_columns) > 1 else fk_columns[0]

                # 在图中创建表节点
                self.builder.add_table_node(table_name, **table_props)

                # --- 阶段 2: 处理列 (Column Nodes) ---
                # 获取 CSV 描述文件中的元数据
                csv_descriptions = self.metadata_manager.get_column_descriptions(table_name)

                for col_name in all_columns:
                    # A. 提取数据 (包含重试逻辑)
                    # 规则：行数>10万则截断读取，否则全量读取 (保留原逻辑)
                    limit = 100000 if row_count > 100000 else None
                    raw_data = db.fetch_column_data(table_name, col_name, limit=limit)

                    # B. 获取元数据状态
                    col_info = db.get_columns_info(table_name)
                    # 从 info 中找到对应列的 type
                    curr_col_type = next((c[2] for c in col_info if c[1] == col_name), "UNKNOWN")

                    is_pk = db.is_primary_key(table_name, col_name)
                    is_fk = db.is_foreign_key(table_name, col_name)
                    is_nullable = db.is_nullable(table_name, col_name)

                    # C. 数据分析 (Data Profiling)
                    # 计算 samples, mean, mode, word_freq 等
                    profile_props = self.profiler.profile(raw_data, curr_col_type, col_name)

                    # D. 合并属性
                    # 基础属性
                    final_props = {
                        "data_type": curr_col_type,
                        "is_nullable": is_nullable,
                        **profile_props  # 展开统计属性
                    }

                    # 融合 CSV 描述 (如果有)
                    # 查找当前列是否有 CSV 描述
                    matching_desc = next((d for d in csv_descriptions if d['original_column_name'] == col_name), None)
                    if matching_desc:
                        if matching_desc.get("column_description"):
                            final_props["column_description"] = matching_desc["column_description"].replace('\n', '')
                        if matching_desc.get("value_description"):
                            final_props["value_description"] = matching_desc["value_description"]

                    # E. 写入图
                    self.builder.add_column_node(
                        table_name,
                        col_name,
                        is_primary_key=is_pk,
                        is_foreign_key=is_fk,
                        **final_props
                    )

            # --- 阶段 3: 处理外键关系 (Edges) ---
            # 必须在所有节点创建完后进行，否则引用计数可能不准确
            print("Processing Foreign Keys...")
            for table_name in tables:
                fks = db.get_foreign_keys(table_name)
                # fks 结构: (id, seq, table, from, to, ...)
                for fk in fks:
                    from_column = fk[3]
                    to_table = fk[2]
                    to_column = fk[4]

                    # 如果目标列是 None (SQLite 特性)，尝试推断为主键
                    if to_column is None:
                        target_pks = db.get_primary_key_columns(to_table)
                        if target_pks:
                            to_column = target_pks[0]  # 假设单主键

                    if to_column:
                        self.builder.add_foreign_key(
                            from_table=table_name,
                            from_column=from_column,
                            to_table=to_table,
                            to_column=to_column
                        )

        # 4. 保存图结构
        self.builder.save_graph(self.output_path)
        print(f"Pipeline completed. Schema graph saved to {self.output_path}")

    @staticmethod
    def load_graph(path):
        """
        辅助方法：供下游任务读取图结构
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Graph file not found: {path}")

        with open(path, 'rb') as f:
            G = pickle.load(f)
        return G


# --- 使用示例 (对应原代码的 main 部分) ---
if __name__ == "__main__":
    # 配置路径
    # database_file = os.path.join(paths.PROJECT_ROOT, "data/spider/medicine_enzyme_interaction.sqlite")
    # output_graph_file = os.path.join(paths.PROJECT_ROOT, "output/spider/sfda/medicine_enzyme_interaction.pkl")
    database_file = "F:/train_bird/train_databases/chicago_crime/chicago_crime.sqlite"
    output_graph_file = "./output/bird/chicago_crime/chicago_crime.pkl"

    # 运行 Pipeline
    pipeline = SchemaPipeline(database_file, output_graph_file)
    pipeline.run()

    # 测试读取
    G = SchemaPipeline.load_graph(output_graph_file)
    print(f"Loaded graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

    # 打印所有节点详细信息
    for node in G.nodes:
        print(G.nodes[node])
    # 打印所有边详细信息
    for edge in G.edges:
        print(G.edges[edge])
