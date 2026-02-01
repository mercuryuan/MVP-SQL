from ast import main
from typing import List, Dict, Any
import logging
from graph_explorer import GraphExplorer
import networkx as nx

from src.utils import GraphLoader

logger = logging.getLogger(__name__)

class SchemaGenerator:
    def __init__(self, graph: nx.DiGraph):
        """
        Initialize SchemaGenerator with a NetworkX graph.
        """
        self.explorer = GraphExplorer(graph)
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
        Generate structured text description for a table.
        """
        table_info = self.tables.get(table_name, {})
        if not table_info:
            return f"# Table: {table_name} (No information available)"
        
        table_name = table_info.get("name", "Unknown Table")
        columns = table_info.get("columns", [])
        primary_key = table_info.get("primary_key")
        foreign_keys = table_info.get("foreign_key")
        row_count = table_info.get("row_count")
        # In pipeline.py, 'column_count' is set.
        column_count = table_info.get("column_count", len(columns))
        description = table_info.get("description")

        reference_paths = []
        if selected_tables:
            for table in selected_tables:
                # Avoid self-reference if present? Or allow it? 
                # Original code allows it (loop over all selected_tables)
                paths = self.explorer.get_foreign_keys_between_tables(table_name, table)
                reference_paths += paths
        else:
            referenced_by = table_info.get("referenced_by", [])
            reference_to = table_info.get("reference_to", [])
            reference_paths = reference_to + referenced_by

        description_lines = [f"# Table: {table_name}", "["]

        if mode == "full":
            if description:
                description_lines.append(f"Description: {description}")
            if columns:
                # columns list might be just names or objects?
                # pipeline.py sets 'columns' as list of strings (names).
                description_lines.append(f"Columns: {', '.join(columns)}")
            if row_count:
                description_lines.append(f"Row Count: {row_count}")

            if reference_paths:
                description_lines.append(f"Reference Path: {reference_paths}")

        elif mode == "brief":
            description_lines = [
                f"# Table: {table_name}",
                f"Columns: {', '.join(columns)}",
                f"Primary Key: {primary_key}" if primary_key else "",
                f"Row Count: {row_count}" if row_count else "",
            ]

        elif mode == "minimal":
            description_lines = [f"# Table: {table_name}", f"Column: {columns}"]

        return "\n".join(filter(None, description_lines))

    def generate_column_description(self, column_info, mode="full"):
        """
        Generate structured description for a column.
        """
        if mode not in ["full", "brief", "minimal"]:
            raise ValueError("mode parameter must be 'full', 'brief' or 'minimal'")
        
        name = column_info.get("name", "Unknown Column")
        data_type = column_info.get("data_type", "Unknown Type")
        base_data_type = data_type.split("(")[0].upper()
        column_description = column_info.get("column_description", None)
        samples = column_info.get("samples", [])
        samples = samples[:6] if samples else None
        samples_str = f"Examples: [{', '.join(map(str, samples))}]" if samples else None

        is_nullable = column_info.get("is_nullable", None)
        if is_nullable is not None:
            nullable_str = "Nullable" if is_nullable else "Not Nullable"
        else:
            nullable_str = None

        integrity_info = []
        if is_nullable:
            data_integrity = column_info.get("data_integrity")
            null_count = column_info.get("null_count")
            if data_integrity is not None:
                integrity_info.append(f"DataIntegrity: {data_integrity}")
                if null_count and null_count != 0:
                    integrity_info.append(f"NullCount: {null_count}")

        # Key Type
        # In pipeline.py, column nodes have 'is_primary_key' and 'is_foreign_key' bools.
        # But 'key_type' list property might not be set explicitly?
        # Let's check pipeline.py again. 
        # pipeline.py does NOT set "key_type" list in final_props.
        # It sets "is_primary_key", "is_foreign_key".
        # So I need to adapt this logic.
        
        key_info = []
        # Original code used column_info.get("key_type", []) which was a list.
        # Now we use booleans if key_type is missing.
        key_type = column_info.get("key_type")
        if key_type:
            if "primary_key" in key_type: key_info.append("Primary Key")
            if "foreign_key" in key_type: key_info.append("Foreign Key")
        else:
            if column_info.get("is_primary_key"): key_info.append("Primary Key")
            if column_info.get("is_foreign_key"): key_info.append("Foreign Key")
            
        key_info_str = ", ".join(key_info) if key_info else None

        value_description = column_info.get("value_description", None)
        value_description_str = f"ValueDescription: {value_description}" if value_description else None

        details = [f"({name}:{base_data_type}"]

        if mode == "minimal":
            return ",".join(details) + ")"

        if mode == "brief":
            if column_description: details.append(column_description)
            if key_info_str: details.append(key_info_str)
            if samples_str: details.append(samples_str)
            if nullable_str: details.append(nullable_str)
            return ",".join(details) + ")"

        if column_description: details.append(column_description)
        if key_info_str: details.append(key_info_str)
        if samples_str: details.append(samples_str)
        if nullable_str: details.append(nullable_str)
        if integrity_info: details.extend(integrity_info)
        # if value_description_str: details.append(value_description_str)

        if base_data_type in self.numeric_types:
            if "numeric_range" in column_info:
                details.append(f"Range: {column_info['numeric_range']}")
            if "numeric_mean" in column_info:
                details.append(f"NumericMean: {column_info['numeric_mean']}")
            if "numeric_mode" in column_info:
                details.append(f"NumericMode: {column_info['numeric_mode']}")

        if base_data_type in self.text_types:
            if "text_categories" in column_info:
                details.append(f"TextCategories: {column_info['text_categories']}")
            if "average_char_length" in column_info:
                details.append(f"AverageCharLength: {column_info['average_char_length']}")
            if "word_frequency" in column_info:
                details.append(f"WordFrequency: {column_info['word_frequency']}")

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
        Generate description for table and its columns.
        """
        descriptions = []
        if selected_tables:
            table_description = self.generate_table_description(table_name, selected_tables=selected_tables)
        else:
            table_description = self.generate_table_description(table_name)
        descriptions.append(table_description)
        
        columns = self.explorer.get_columns_for_table(table_name)
        for col in columns.keys():
            column_description = self.generate_column_description(columns[col], mode=detail_level)
            descriptions.append(column_description)
        return "\n".join(descriptions) + "\n]"

#测试所有功能
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    # 加载图结构
    pkl_path = r"D:\MVP-SQL\output\converted_graph_pkl\bird\books\books.pkl"
    G = GraphLoader.load_graph(pkl_path)
    if G is None:
        logger.error("图结构加载失败，程序退出")
        exit(1)
        
    #打印图结构
    logger.info(f"图结构节点数: {G.number_of_nodes()}")
    logger.info(f"图结构边数: {G.number_of_edges()}")
    sg = SchemaGenerator(G)
    # 打印整个数据库的表
    for table in sg.tables:
        print(sg.generate_combined_description(table))




