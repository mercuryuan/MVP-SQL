import csv
import os
import chardet


class MetadataManager:
    def __init__(self, database_file):
        """
        初始化元数据管理器。

        :param database_file: 数据库文件路径 (用于定位 database_description 目录)
        """
        self.database_file = database_file
        # 获取 database_file 的目录部分
        self.base_dir = os.path.dirname(database_file)

    def get_column_descriptions(self, table_name):
        """
        从对应的 CSV 文件中读取指定表的列描述信息。

        :param table_name: 表名
        :return: 包含列描述信息的字典列表
        """
        # 特殊处理：sqlite_sequence 表直接返回空
        if table_name == "sqlite_sequence":
            return []

        # 构造 CSV 文件路径：数据库同级目录/database_description/{table_name}.csv
        file_path = os.path.join(self.base_dir, "database_description", f"{table_name}.csv")

        column_descriptions = []

        if not os.path.exists(file_path):
            # 原代码逻辑：文件不存在时直接返回空列表 (曾有一行print被原作者注释掉了)
            return []

        try:
            # 策略 1: 尝试使用 utf-8-sig 编码打开 (处理常见的 BOM 问题)
            with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
                return self._parse_csv_content(csvfile)

        except UnicodeDecodeError:
            # 策略 2: 编码错误时，动态检测文件编码
            try:
                detected_encoding = self._detect_encoding(file_path)
                # print(f"检测到文件编码: {detected_encoding} for {file_path}")

                with open(file_path, 'r', encoding=detected_encoding) as csvfile:
                    return self._parse_csv_content(csvfile)

            except Exception as e:
                print(f"读取 CSV 文件时发生错误，尝试动态编码检测后仍失败: {e}, 文件: {file_path}")
                return []

        except Exception as e:
            # 其他 IO 错误
            print(f"读取 CSV 文件异常: {e}, 文件: {file_path}")
            return []

    def _detect_encoding(self, file_path):
        """使用 chardet 检测文件编码"""
        with open(file_path, 'rb') as raw_file:
            raw_data = raw_file.read()
            result = chardet.detect(raw_data)
            return result['encoding']

    def _parse_csv_content(self, csvfile):
        """
        解析 CSV 内容，保留原代码的字段验证逻辑。
        """
        descriptions = []
        reader = csv.DictReader(csvfile)

        expected_keys = [
            'original_column_name',
            'column_name',
            'column_description',
            'data_format',
            'value_description'
        ]

        for row in reader:
            # 完整性校验：确保行数据包含所有预期键
            if all(key in row for key in expected_keys):
                descriptions.append(row)
            else:
                print(f"行数据缺少预期键，已跳过: {row}")

        return descriptions
