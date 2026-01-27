import random
import json
import operator
import numpy as np
from collections import Counter
from decimal import Decimal
from datetime import datetime, date
from dateutil import parser
from .utils import convert_date_string


class DataProfiler:
    def __init__(self):
        # 定义类型分类常量
        self.numeric_types = [
            "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
            "REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BOOLEAN"
        ]
        self.text_types = [
            "TEXT", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR", "NTEXT",
            "CLOB", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "JSON", "XML"
        ]
        self.time_types = ["DATE", "DATETIME", "TIMESTAMP"]

    def profile(self, all_values, data_type, column_name=""):
        """
        对列数据进行分析，返回统计属性字典。
        对应原代码 _get_column_samples_and_attributes 中非 SQL 的部分。
        """
        attributes = {}
        base_data_type = data_type.split('(')[0].upper()

        # 1. 空值过滤与完整性统计
        def is_empty(value):
            return value is None or (isinstance(value, str) and value.strip() == "")

        non_null_values = [v for v in all_values if not is_empty(v)]

        null_count = len(all_values) - len(non_null_values)
        attributes['null_count'] = null_count
        attributes['data_integrity'] = "{:.0f}%".format(
            len(non_null_values) / len(all_values) * 100) if all_values else "100%"
        attributes['sample_count'] = len(non_null_values) if non_null_values else 0

        # 2. 采样 (Sampling)
        samples = []
        if base_data_type in self.text_types:
            # 文本类型随机抽样最多 6 条
            samples = random.sample(non_null_values, min(len(non_null_values), 6))
            # 文本截断处理 (保留原逻辑: >30 字符截断)
            max_length = 30
            samples = [s[:max_length] + '...' if len(s) > max_length else s for s in samples]
        else:
            # 其他类型随机抽样最多 6 条
            samples = random.sample(non_null_values, min(len(non_null_values), 6))

        attributes['samples'] = samples

        # 3. 类型特定的统计分析
        if base_data_type in self.numeric_types:
            self._analyze_numeric(non_null_values, base_data_type, column_name, attributes)

        elif base_data_type in self.text_types:
            self._analyze_text(non_null_values, attributes)

        elif base_data_type in self.time_types:
            self._analyze_time(non_null_values, attributes)

        return attributes

    def _analyze_numeric(self, values, data_type, column_name, attributes):
        """数值类型分析逻辑"""
        # 过滤掉非数值型数据
        valid_values = [v for v in values if isinstance(v, (int, float)) and v != '']

        # 保留原代码警告逻辑
        filtered_count = len(values) - len(valid_values)
        if filtered_count > 0:
            print(f"过滤掉了 {column_name} 的 {filtered_count} 个非数值数据")

        try:
            # 计算范围
            attributes['numeric_range'] = [min(valid_values), max(valid_values)] if valid_values else None

            is_id_column = "id" in column_name.lower()
            if not is_id_column:
                # 计算众数
                mode = self._get_mode(valid_values)
                if mode:
                    attributes['numeric_mode'] = mode

                # 计算平均值
                if valid_values:
                    try:
                        if data_type in ["DECIMAL", "NUMERIC"]:
                            # Decimal 转换处理
                            attributes['numeric_mean'] = float(
                                np.mean([float(Decimal(str(v))) for v in valid_values]))
                        elif data_type == "BOOLEAN":
                            attributes['numeric_mean'] = float(
                                np.mean([int(v) for v in valid_values]))
                        else:
                            attributes['numeric_mean'] = float(np.mean(valid_values))
                    except Exception as e:
                        print(f"计算平均值时出错: {e}, 列名: {column_name}")
                        attributes['numeric_mean'] = None
                else:
                    attributes['numeric_mean'] = None
        except Exception as e:
            print(f"计算数值范围时出错: {e}, 列名: {column_name}")
            attributes['numeric_range'] = None

    def _analyze_text(self, values, attributes):
        """文本类型分析逻辑"""
        # 类别型数据检测 (唯一值 <= 6)
        if len(set(values)) <= 6:
            attributes['text_categories'] = list(set(values))

        # 平均字符长度
        attributes['average_char_length'] = self._get_average_char_length(values) if values else 0

        # 词频统计
        word_frequency_dict = self._get_word_frequency(values) if values else {}
        attributes['word_frequency'] = json.dumps(word_frequency_dict, ensure_ascii=False)

    def _analyze_time(self, values, attributes):
        """时间类型分析逻辑"""
        if values:
            attributes['time_span'] = self._get_time_span(values)
            time_attributes = self._calculate_time_attributes(values)
            attributes.update(time_attributes)
        else:
            attributes['time_span'] = None

    # --- 私有辅助方法 (原样迁移) ---

    def _get_mode(self, values):
        """获取众数，支持返回多个众数"""
        if not values:
            return []
        if all(isinstance(v, Decimal) for v in values):
            values = [float(v) for v in values]
        if all(isinstance(v, bool) for v in values):
            values = [int(v) for v in values]

        count_dict = Counter(values)
        max_count = max(count_dict.values())
        if max_count <= 1:
            return []
        return [k for k, v in count_dict.items() if v == max_count]

    def _get_average_char_length(self, values):
        if values:
            total_length = sum(len(v) for v in values)
            return total_length / len(values)
        return 0

    def _get_word_frequency(self, values, top_k=10, by_word=False):
        """
        统计词频。
        **关键保留**: 频率为1的词最多保留3个且长度不超过20的逻辑。
        """
        if not values:
            return {}

        if by_word:
            all_words = " ".join(values).split()
            word_count_dict = Counter(all_words)
        else:
            word_count_dict = Counter(values)

        sorted_word_count_dict = dict(
            sorted(word_count_dict.items(), key=operator.itemgetter(1), reverse=True)
        )

        result = {}
        one_freq_count = 0
        found_one_freq = False

        for word, freq in sorted_word_count_dict.items():
            if len(result) >= top_k:
                break
            if freq == 1:
                found_one_freq = True
                if len(word) <= 20 and one_freq_count < 3:
                    result[word] = freq
                    one_freq_count += 1
            else:
                if not found_one_freq:
                    result[word] = freq
        return result

    def _get_time_span(self, values):
        """计算时间跨度"""
        if values:
            datetime_values = [convert_date_string(v) for v in values if convert_date_string(v) is not None]
            if datetime_values:
                min_time = min(datetime_values)
                max_time = max(datetime_values)
                time_diff = max_time - min_time
                return f"{time_diff.days} days"
        return None

    def _calculate_time_attributes(self, values):
        """计算最早和最晚时间"""
        parsed_values = []
        for v in values:
            if isinstance(v, str):
                if "T" in v:
                    try:
                        parsed = parser.isoparse(v)
                    except ValueError:
                        parsed = None
                else:
                    parsed = convert_date_string(v)
            elif isinstance(v, datetime) or isinstance(v, date):
                parsed = v
            else:
                parsed = None

            if parsed:
                parsed_values.append(parsed)

        if parsed_values:
            def format_value(value):
                # 只有 date 且不是 datetime 时才只返回日期部分
                if isinstance(value, date) and not isinstance(value, datetime):
                    return value.strftime('%Y-%m-%d')
                else:
                    return value.strftime('%Y-%m-%d %H:%M:%S')

            return {
                'earliest_time': format_value(min(parsed_values)),
                'latest_time': format_value(max(parsed_values))
            }
        else:
            return {'earliest_time': None, 'latest_time': None}
