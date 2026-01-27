import hashlib
from datetime import datetime, date


def convert_date_string(date_str):
    """
    尝试将输入的日期字符串按照多种常见格式转换为datetime对象或date对象。
    完全保留原代码的格式列表和转换逻辑。
    """
    # 检查 date_str 是否为字符串类型
    if not isinstance(date_str, str):
        try:
            date_str = str(date_str)
        except Exception as e:
            # print(f"无法将输入 {date_str} 转换为字符串类型，错误信息: {e}")
            return None

    date_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%Y.%m.%d',
        '%m/%d/%Y',
        '%m-%d-%Y',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%Y.%m.%d %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%m-%d-%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',  # 新增的日期时间格式
        '%Y/%m/%d %H:%M:%S.%f',
        '%Y.%m.%d %H:%M:%S.%f',
        '%m/%d/%Y %H:%M:%S.%f',
        '%m-%d-%Y %H:%M:%S.%f',
        '%d/%m/%Y %H:%M:%S.%f',
        '%d-%m-%Y %H:%M:%S.%f',
        '%Y'  # 仅年份的格式
    ]
    for format_str in date_formats:
        try:
            dt = datetime.strptime(date_str, format_str)
            # 如果格式是纯日期（没有时间部分），返回 date 对象
            if format_str in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y']:
                if format_str == '%Y':
                    # 对于仅年份的格式，将其转换为该年的 1 月 1 日
                    return date(dt.year, 1, 1)
                return dt.date()
            return dt
        except ValueError:
            continue
    return None


def generate_fk_hash(table1, column1, table2, column2):
    """
    生成无序的外键ID，用于唯一标识外键关系。
    """
    elements = sorted([f"{table1}.{column1}", f"{table2}.{column2}"])
    unique_string = "|".join(elements)
    return hashlib.md5(unique_string.encode()).hexdigest()
