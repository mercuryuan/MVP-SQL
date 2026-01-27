import sqlite3
import os


def quote_identifier(identifier):
    """
    引用标识符（表名或列名），防止包含空格或特殊字符时出错。
    """
    return f'"{identifier}"'


class SQLiteHandler:
    def __init__(self, database_file):
        """
        初始化 SQLite 处理器。
        注意：此处不再直接连接，建议使用 with 语句管理生命周期。
        """
        self.database_file = database_file
        if not os.path.exists(database_file):
            raise FileNotFoundError(f"Database file not found: {database_file}")

        self.conn = None
        self.cursor = None

    def __enter__(self):
        """
        进入上下文管理器：建立连接
        """
        # check_same_thread=False 允许在不同线程使用连接（虽然这里主要是单线程）
        # isolation_level=None 开启自动提交模式，避免事务锁死
        self.conn = sqlite3.connect(self.database_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文管理器：关闭连接，确保释放文件锁
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

        self.cursor = None
        self.conn = None

    def _ensure_connection(self):
        """
        辅助检查：如果用户没有使用 with 语句，而是直接调用方法，则报错或自动连接。
        这里选择报错以强制规范使用。
        """
        if self.conn is None or self.cursor is None:
            raise RuntimeError(
                "Database not connected. Please use 'with SQLiteHandler(...) as handler:' context manager.")

    def get_database_name(self):
        """从文件路径提取数据库名称"""
        file_name = os.path.basename(self.database_file)
        return os.path.splitext(file_name)[0]

    def get_all_tables(self):
        """获取数据库中所有的表名"""
        self._ensure_connection()
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        self.cursor.execute(query)
        tables = self.cursor.fetchall()
        return [t[0] for t in tables if t[0] != "sqlite_sequence"]

    def get_row_count(self, table_name):
        """获取表的数据行数"""
        self._ensure_connection()
        query = f"SELECT COUNT(*) FROM {quote_identifier(table_name)}"
        try:
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"获取表 {table_name} 数据条数时出错: {e}")
            return None

    def get_columns_info(self, table_name):
        """获取表的列元数据 (PRAGMA table_info)"""
        self._ensure_connection()
        query = f"PRAGMA table_info({quote_identifier(table_name)})"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_foreign_keys(self, table_name):
        """获取表的外键信息 (PRAGMA foreign_key_list)"""
        self._ensure_connection()
        query = f"PRAGMA foreign_key_list({quote_identifier(table_name)})"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetch_column_data(self, table_name, column_name, limit=None):
        """
        获取列的具体数据。

        **优化后的 Fallback 逻辑**:
        不重新连接，而是临时修改当前连接的 text_factory。
        """
        self._ensure_connection()

        if limit is not None:
            query = f"SELECT {quote_identifier(column_name)} FROM {quote_identifier(table_name)} LIMIT {limit};"
        else:
            query = f"SELECT {quote_identifier(column_name)} FROM {quote_identifier(table_name)} ;"

        all_values = []

        try:
            # 正常模式：text_factory 默认为 str
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            all_values = [row[0] for row in rows]

        except sqlite3.OperationalError as e:
            print(f"警告：读取 {table_name}.{column_name} 失败，切换 bytes 模式重试。错误: {e}")

            # 保存原本的 factory (通常是 str)
            original_factory = self.conn.text_factory

            try:
                # 临时切换为 bytes 模式
                self.conn.text_factory = bytes
                # 需要重新获取 cursor 或者直接用 conn execute，为了保险起见重新 execute
                self.cursor.execute(query)
                rows = self.cursor.fetchall()

                all_values = []
                for row in rows:
                    value = row[0]
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='ignore')
                    all_values.append(value)

            except Exception as e_fallback:
                print(f"使用 bytes 方式重试依然失败: {e_fallback}")
                return []
            finally:
                # **必须恢复** text_factory，否则影响后续查询
                self.conn.text_factory = original_factory

        return all_values

    # --- 辅助判断方法 ---

    def is_primary_key(self, table_name, column_name):
        columns_info = self.get_columns_info(table_name)
        for column_info in columns_info:
            if column_info[1] == column_name:
                return bool(column_info[5])
        return False

    def is_foreign_key(self, table_name, column_name):
        fks = self.get_foreign_keys(table_name)
        for fk in fks:
            if fk[3] == column_name:
                return True
        return False

    def is_nullable(self, table_name, column_name):
        columns_info = self.get_columns_info(table_name)
        for column_info in columns_info:
            if column_info[1] == column_name:
                return not column_info[3]
        return None

    def get_primary_key_columns(self, table_name):
        columns_info = self.get_columns_info(table_name)
        return [info[1] for info in columns_info if info[5] != 0]

    def get_foreign_key_columns(self, table_name):
        fks = self.get_foreign_keys(table_name)
        return [fk[3] for fk in fks]
