import json
from typing import Optional, List, Dict

from configs.paths import (
    SPIDER_TRAIN_JSON,
    SPIDER_TRAIN_OTHER_JSON,
    SPIDER_DEV_JSON,
    BIRD_TRAIN_JSON,
    BIRD_DEV_JSON,
)


class DataLoader:
    """
    科研级通用数据加载器
    功能：统一 Spider 和 BIRD 数据集的字段映射，支持按需提取、数据库筛选及字段补齐。
    """

    # 数据集路径配置映射
    DATASETS = {
        "spider_train": SPIDER_TRAIN_JSON,
        "spider_other": SPIDER_TRAIN_OTHER_JSON,
        "spider_dev": SPIDER_DEV_JSON,
        "bird": BIRD_TRAIN_JSON,
        "bird_dev": BIRD_DEV_JSON,
    }

    # 字段别名映射：{ 原始字段名: 统一后的目标名 }
    COLUMN_MAPPING = {
        "query": "sql_query",  # Spider 的原始 SQL
        "SQL": "sql_query",  # BIRD 的原始 SQL
        "question": "question",
        "db_id": "db_id",
        "evidence": "evidence"  # BIRD 特有字段
    }

    def __init__(self, dataset_name: str):
        """
        初始化加载器
        :param dataset_name: 支持 spider, spider_dev, bird 等
        """
        if dataset_name == "spider":
            self.data = self._merge_json_files(SPIDER_TRAIN_JSON, SPIDER_TRAIN_OTHER_JSON)
            self.dataset_name = "spider_full_train"
        elif dataset_name in self.DATASETS:
            self.data = self._load_data(self.DATASETS[dataset_name])
            self.dataset_name = dataset_name
        else:
            raise ValueError(f"未知数据集: {dataset_name}，支持: {list(self.DATASETS.keys()) + ['spider']}")

    def _load_data(self, file_path: str) -> List[Dict]:
        """从 JSON 文件读取数据"""
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _merge_json_files(self, path1: str, path2: str) -> List[Dict]:
        """合并两个 JSON 文件"""
        return self._load_data(path1) + self._load_data(path2)

    def filter_data(self, db_id: Optional[str] = None, fields: Optional[List[str]] = None, show_count: bool = False):
        """
        统一提取数据字段
        :param db_id: 过滤指定的数据库 ID
        :param fields: 用户想要保留的统一字段名（如 ['question', 'sql_query', 'evidence']）
        :param show_count: 是否打印处理后的条数
        """
        # 1. 基础数据库筛选
        filtered = [item for item in self.data if item.get("db_id") == db_id] if db_id else self.data

        processed_data = []
        for item in filtered:
            # 如果没有指定 fields，则保留原始 item 的副本
            if not fields:
                processed_data.append(item.copy())
                continue

            # 2. 字段映射与补齐逻辑
            new_item = {}

            # 建立一个临时映射，方便快速查找原始数据中有哪些统一后的字段
            # 例如 { "sql_query": "SELECT...", "question": "What..." }
            current_mapped_values = {self.COLUMN_MAPPING.get(k, k): v for k, v in item.items()}

            for field in fields:
                # 获取映射后的值
                val = current_mapped_values.get(field)

                # 核心处理：如果请求了 evidence 但原始数据没有（如 Spider），则补 None
                if field == "evidence" and val is None:
                    new_item["evidence"] = None
                # 处理 SQL 清洗：去掉分号和多余空格
                elif field == "sql_query" and isinstance(val, str):
                    new_item["sql_query"] = val.strip().rstrip(';')
                else:
                    new_item[field] = val

            processed_data.append(new_item)

        if show_count:
            print(f"[{self.dataset_name}] 筛选/处理完成，共 {len(processed_data)} 条数据。")

        return processed_data

    def list_dbnames(self):
        """列出当前数据集中包含的所有数据库 ID"""
        db_ids = sorted(list({item.get("db_id") for item in self.data if "db_id" in item}))
        return db_ids


if __name__ == '__main__':
    # 验证接口一致性
    # 1. 测试 Spider (会自动补全 evidence 为 None)
    spider_loader = DataLoader("spider_dev")
    spider_res = spider_loader.filter_data(fields=["question", "sql_query", "evidence"], show_count=True)
    print("Spider 样例 (带 evidence 补全):", spider_res[0])

    # 2. 测试 BIRD (会保留原始 evidence 内容)
    bird_loader = DataLoader("bird")
    bird_res = bird_loader.filter_data(db_id="superstore", fields=["question", "sql_query", "evidence"],
                                       show_count=True)
    if bird_res:
        print("BIRD 样例 (原生 evidence):", bird_res[0])