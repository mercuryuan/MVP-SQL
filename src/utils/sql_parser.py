import pickle
import re
import sys
import os
import networkx as nx
import sqlglot
from sqlglot.optimizer.qualify import qualify
from sqlglot.errors import OptimizeError
from sqlglot.expressions import Table, Column, Join, Where, Identifier, EQ
from typing import Dict, List, Tuple, Set, Any, Optional

# 将项目根目录添加到 sys.path 以便导入 src
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.graph_schema_extractor import GraphSchemaExtractor


class SQLParser:
    """
    SQL 解析器，用于分析 SQL 语句中的实体（表、列）、关系（JOIN、条件）等信息。
    基于 GraphSchemaExtractor 提供的 Schema 进行实体校正和补全。
    """

    def __init__(self, dataset_name: str, db_name: str):
        """
        初始化 SQL 解析器。
        
        :param dataset_name: 数据集名称 (e.g., 'bird', 'spider')
        :param db_name: 数据库名称
        """
        self.dataset_name = dataset_name
        self.db_name = db_name
        
        # 1. 加载 Schema
        self.extractor = GraphSchemaExtractor(dataset_name)
        # raw_schema 格式: {table_name: [col1, col2, ...]}
        self.raw_schema = self.extractor.extract_schema(db_name)
        
        # 2. 构建用于 sqlglot optimizer 的 schema 字典
        # 格式: {table: {col: type}}
        self.optimizer_schema = {}
        # 辅助结构：用于不区分大小写的查找
        self._table_map_lower = {} # lower -> original
        self._col_map_lower = {}   # table_lower -> {col_lower: original_col}

        for table, cols in self.raw_schema.items():
            self.optimizer_schema[table] = {col: "text" for col in cols}  # 类型默认为 text
            
            t_lower = table.lower()
            self._table_map_lower[t_lower] = table
            self._col_map_lower[t_lower] = {c.lower(): c for c in cols}

        # 3. 加载外键信息
        self.foreign_keys = self.extractor.extract_foreign_keys(db_name)

    def parse_sql(self, sql: str) -> sqlglot.Expression:
        """
        解析 SQL 语句，并进行 Schema 校正（Qualify）。
        如果校正失败（通常是因为找不到列或表），会抛出异常。
        """
        try:
            expression = sqlglot.parse_one(sql)
            # 使用 sqlglot 的 qualify 进行优化和校正
            qualified_expression = qualify(expression, schema=self.optimizer_schema)
            return qualified_expression
        except OptimizeError as e:
            # 将 sqlglot 的优化错误转换为 ValueError，提供更友好的提示
            raise ValueError(f"SQL validation failed: {e}")

    def extract_entities(self, sql: str) -> Dict[str, List[str]]:
        """
        提取 SQL 中的实体信息，按表进行分组。
        并验证实体是否存在于 Schema 中，如果不存在则抛出 ValueError。
        """
        # 1. 解析 SQL (这一步如果 qualify 失败会直接报错)
        expression = self.parse_sql(sql)
        
        alias_to_table, _ = self._extract_table_info(expression)
        
        # 2. 验证表名是否存在 (不区分大小写)
        real_table_names = {} # alias -> real_table_name
        for alias, table_name in alias_to_table.items():
            t_lower = table_name.lower()
            if t_lower not in self._table_map_lower:
                raise ValueError(f"Table not found in schema: {table_name}")
            real_table_names[alias] = self._table_map_lower[t_lower]

        # 初始化结果字典
        # 使用真实表名作为 key
        table_columns = {real_name: set() for real_name in real_table_names.values()}
        
        for column in expression.find_all(Column):
            table_alias = column.table
            real_table = None
            
            if table_alias:
                # 如果有表别名/表名限定
                if table_alias in real_table_names:
                     real_table = real_table_names[table_alias]
                # 这里的 else 情况已经被 parse_sql/qualify 处理了，理论上不会出现未知 alias
            
            if real_table:
                col_name = column.name
                if col_name == "*":
                    continue
                
                # 验证列名 (不区分大小写)
                c_lower = col_name.lower()
                t_lower = real_table.lower()
                
                # 注意：qualify 之后，列名通常已经是正确的，但为了保险再次检查
                if c_lower not in self._col_map_lower[t_lower]:
                     raise ValueError(f"Column not found in schema: {real_table}.{col_name}")
                
                # 存储真实列名
                real_col = self._col_map_lower[t_lower][c_lower]
                table_columns[real_table].add(real_col)

        # 转换为列表并排序
        return {k: sorted(list(v)) for k, v in table_columns.items()}

    def generate_report(self, sql: str) -> str:
        """
        生成 SQL 解析的完整格式化报告。
        包括实体解析和关系解析。
        如果实体不存在，会抛出 ValueError。
        """
        # 1. 提取实体（包含存在性验证）
        entities = self.extract_entities(sql)
        
        # 2. 提取关系
        relationships = self.extract_relationships(sql)
        
        # 3. 格式化输出
        report = []
        report.append(self.format_entities(entities))
        report.append("\n" + self.format_relationships(relationships))
        
        return "\n".join(report)

    def extract_relationships(self, sql: str) -> Dict[str, Any]:
        """
        提取 SQL 中的关系信息（JOINs, Where 条件）。
        """
        expression = self.parse_sql(sql)
        alias_to_table, _ = self._extract_table_info(expression)
        
        # 建立别名到真实表名的映射（处理大小写）
        alias_to_real_table = {}
        for alias, table_name in alias_to_table.items():
            t_lower = table_name.lower()
            if t_lower in self._table_map_lower:
                alias_to_real_table[alias] = self._table_map_lower[t_lower]
            else:
                # 理论上 extract_entities 会先检查，但如果单独调用此方法，也应保留原名或报错
                # 这里我们选择保留原名，或者抛错？
                # 为了保持一致性，如果找不到表，抛错
                raise ValueError(f"Table not found in schema: {table_name}")

        joins = self._extract_join_relationships(expression, alias_to_real_table)
        conditions = self._extract_where_conditions(expression, alias_to_real_table)
        
        return {
            "joins": joins,
            "conditions": conditions
        }

    def format_entities(self, entities: Dict[str, List[str]]) -> str:
        """
        格式化输出实体信息，展示表列从属关系。
        
        :param entities: extract_entities 返回的字典 {table: [columns]}
        :return: 格式化后的字符串
        """
        if not entities:
            return "无相关数据库实体。"

        lines = ["涉及的数据库实体："]
        for i, (table, columns) in enumerate(entities.items(), 1):
            lines.append(f"{i}. 表 {table}")
            if columns:
                for col in columns:
                    lines.append(f"   - {col}")
            else:
                lines.append("   - (仅涉及表引用，无显式列引用)")
        
        return "\n".join(lines)

    def format_relationships(self, relationships: Dict[str, Any]) -> str:
        """
        格式化输出关系信息。
        """
        lines = ["涉及的关系信息："]
        
        joins = relationships.get("joins", [])
        if joins:
            lines.append("1. JOIN 连接：")
            for join in joins:
                lines.append(f"   - 类型: {join['join_type']}")
                lines.append(f"   - 条件: {join['on']}")
                
                # 命中外键时不显示（避免重复），未命中时提示
                if not join.get('fk_matches'):
                     lines.append(f"     [提示]: 未命中已知外键关系")
        else:
            lines.append("1. JOIN 连接：无")

        conditions = relationships.get("conditions", [])
        if conditions:
            lines.append("2. WHERE 筛选：")
            for cond in conditions:
                lines.append(f"   - {cond}")
        else:
            lines.append("2. WHERE 筛选：无")
            
        return "\n".join(lines)

    # --- 内部辅助方法 ---

    def _extract_table_info(self, expression: sqlglot.Expression) -> Tuple[Dict[str, str], Set[str]]:
        """
        提取表信息。
        :return: (alias_to_table_dict, set_of_table_names)
        """
        alias_to_table = {}
        tables = set()
        
        for table in expression.find_all(Table):
            table_name = table.name
            alias = table.alias
            
            # 记录映射: 别名 -> 真名
            if alias:
                alias_to_table[alias] = table_name
            
            # 始终记录 table_name 本身指向自己
            alias_to_table[table_name] = table_name
            tables.add(table_name)
            
        return alias_to_table, tables

    def _extract_join_relationships(self, expression: sqlglot.Expression, alias_to_table: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        提取 JOIN 关系，并将别名还原为表名。
        同时分析是否命中外键。
        """
        joins = []
        seen_conditions = set()

        for join in expression.find_all(Join):
            join_type = join.kind
            on_condition = join.args.get("on")

            if on_condition:
                on_condition_str = self._resolve_aliases_in_expression(on_condition, alias_to_table)

                if on_condition_str in seen_conditions:
                    continue
                
                seen_conditions.add(on_condition_str)
                
                # 分析外键路径
                fk_matches = self._analyze_foreign_key_path(on_condition, alias_to_table)
                
                joins.append({
                    "join_type": join_type,
                    "on": on_condition_str,
                    "fk_matches": fk_matches
                })
        return joins

    def _analyze_foreign_key_path(self, expression: sqlglot.Expression, alias_to_table: Dict[str, str]) -> List[str]:
        """
        分析连接条件是否命中外键。
        返回命中外键的描述列表。
        """
        fk_paths = []
        
        # 查找表达式中的所有等值比较
        comparisons = []
        if isinstance(expression, EQ):
            comparisons.append(expression)
        else:
            comparisons.extend(expression.find_all(EQ))
            
        for eq in comparisons:
            left = eq.left
            right = eq.right
            
            if isinstance(left, Column) and isinstance(right, Column):
                l_table = alias_to_table.get(left.table, left.table)
                l_col = left.name
                r_table = alias_to_table.get(right.table, right.table)
                r_col = right.name
                
                # Check against FKs
                for fk in self.foreign_keys:
                    # Normalize to lowercase for comparison
                    fk_from_table = fk['from_table'].lower()
                    fk_from_col = fk['from_column'].lower()
                    fk_to_table = fk['to_table'].lower()
                    fk_to_col = fk['to_column'].lower()
                    
                    l_table_lower = str(l_table).lower()
                    l_col_lower = str(l_col).lower()
                    r_table_lower = str(r_table).lower()
                    r_col_lower = str(r_col).lower()

                    # Case 1: left matches from, right matches to
                    match_forward = (l_table_lower == fk_from_table and l_col_lower == fk_from_col and 
                                     r_table_lower == fk_to_table and r_col_lower == fk_to_col)
                    
                    # Case 2: left matches to, right matches from
                    match_backward = (l_table_lower == fk_to_table and l_col_lower == fk_to_col and 
                                      r_table_lower == fk_from_table and r_col_lower == fk_from_col)
                    
                    if match_forward or match_backward:
                        path_str = f"{fk['from_table']}.{fk['from_column']} -> {fk['to_table']}.{fk['to_column']}"
                        fk_paths.append(path_str)
                        
        return list(set(fk_paths))

    def _extract_where_conditions(self, expression: sqlglot.Expression, alias_to_table: Dict[str, str]) -> List[str]:
        """
        提取 WHERE 条件，并将别名还原为表名。
        """
        conditions = []
        where_clause = expression.find(Where)
        if where_clause:
            condition_expr = where_clause.this
            condition_str = self._resolve_aliases_in_expression(condition_expr, alias_to_table)
            conditions.append(condition_str)
        return conditions

    def _resolve_aliases_in_expression(self, expression: sqlglot.Expression, alias_to_table: Dict[str, str]) -> str:
        """
        将表达式中的表别名替换为真实表名。
        """
        expr_copy = expression.copy()
        for col in expr_copy.find_all(Column):
            if col.table and col.table in alias_to_table:
                col.set("table", Identifier(this=alias_to_table[col.table], quoted=False))
            # 同时也确保列名不带引号
            col.set("this", Identifier(this=col.name, quoted=False))
            
        return expr_copy.sql(identify=False)


if __name__ == '__main__':
    # 测试代码
    try:
        parser = SQLParser("bird", "books")
        
        test_sql = "SELECT T2.publisher_name FROM book AS T1 INNER JOIN publisher AS T2 ON T1.publisher_id = T2.publisher_id WHERE T1.title = 'The Illuminati'"
        
        print(f"SQL: {test_sql}\n")
        
        # 1. 提取并分组实体
        entities = parser.extract_entities(test_sql)
        
        # 2. 格式化输出
        print(parser.format_entities(entities))

        
        # 3. 关系提取
        relationships = parser.extract_relationships(test_sql)
        print(parser.format_relationships(relationships))

        # 直接调用
        sql = """
        SELECT T1.title FROM book AS T1 INNER JOIN publisher AS T2 ON T1.publisher_id = T2.publisher_id WHERE T2.publisher_name = 'Thomas Nelson' ORDER BY T1.publication_date ASC LIMIT 1
        """
        print(parser.generate_report(sql))

    except Exception as e:
        print(f"Error: {e}")
