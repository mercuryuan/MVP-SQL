import json
import logging
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

# 尝试导入配置，如果不存在则使用占位符，防止报错影响阅读
from configs.paths import (
    SPIDER_TRAIN_JSON, SPIDER_DEV_JSON, SPIDER_TRAIN_OTHER_JSON,
    BIRD_TRAIN_JSON, BIRD_DEV_JSON
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataLoader:
    """
    通用 Text-to-SQL 数据加载器。

    特点：
    1. 支持自动合并多个源文件（如 Spider 的 train + others）。
    2. 自动标准化字段名（将 query/SQL 统一为 sql）。
    3. 支持像 List 一样操作 (len(), loader[0])。
    可选数据集：
    1. spider：Spider 数据集，包含 train + others。
    2. spider_dev：Spider 开发集。
    3. spider_train：Spider 训练集（train.json）。
    4. bird：BIRD 数据集（train.json）。
    5. bird_dev：BIRD 开发集（dev.json）。
    """
    # 数据集配置：支持单个文件路径或文件路径列表
    # 如果是列表，加载时会自动合并数据
    DATASET_CONFIG: Dict[str, Union[str, List[str]]] = {
        "spider": [SPIDER_TRAIN_JSON, SPIDER_TRAIN_OTHER_JSON],  # 自动合并这两个文件
        "spider_dev": SPIDER_DEV_JSON,
        "spider_train": SPIDER_TRAIN_JSON,
        "bird": BIRD_TRAIN_JSON,
        "bird_dev": BIRD_DEV_JSON,
    }

    # 需要被标准化为 'sql' 的别名集合
    SQL_ALIASES = {"query", "sql", "SQL"}

    def __init__(self, dataset_name: str, auto_load: bool = True):
        """
        初始化加载器。
        可选数据集：
            1. spider：Spider 数据集，包含 train + others。
            2. spider_dev：Spider 开发集。
            3. spider_train：Spider 训练集（train.json）。
            4. bird：BIRD 数据集（train.json）。
            5. bird_dev：BIRD 开发集（dev.json）。
        Args:
            dataset_name: 数据集名称 (key in DATASET_CONFIG)
            auto_load: 是否在初始化时立即加载数据到内存，默认为 True
        """
        if dataset_name not in self.DATASET_CONFIG:
            raise ValueError(f"不支持的数据集: '{dataset_name}'。可选: {list(self.DATASET_CONFIG.keys())}")

        self.dataset_name = dataset_name
        self._raw_data: List[Dict[str, Any]] = []

        if auto_load:
            self.load()

    def load(self):
        """执行数据加载操作"""
        paths = self.DATASET_CONFIG[self.dataset_name]

        # --- 修改这里 ---
        # 兼容字符串和 pathlib.Path 对象
        # 如果不是列表（即单个路径），则包装成列表
        if not isinstance(paths, list):
            paths = [paths]
        # ----------------

        self._raw_data = []
        for path in paths:
            self._raw_data.extend(self._read_json(path))

        logger.info(f"数据集 [{self.dataset_name}] 加载完成，共 {len(self._raw_data)} 条数据。")

    def _read_json(self, file_path: Union[str, Path]) -> List[Dict]:
        """读取单个 JSON 文件，包含基础的错误处理"""
        # 确保它是一个 Path 对象
        path_obj = Path(file_path)

        if not path_obj.exists():
            # 报错时打印绝对路径，方便你排查到底是在哪个目录下找不到文件
            logger.warning(f"文件不存在: {path_obj.absolute()}，跳过加载。")
            return []

        try:
            with open(path_obj, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.error(f"文件格式错误（非标准 JSON）: {file_path}")
            return []

    def filter(self,
               db_id: Optional[str] = None,
               fields: Optional[List[str]] = None,
               verbose: bool = False) -> List[Dict[str, Any]]:
        """
        核心方法：过滤数据并提取特定字段。
        会自动将不同数据集的 SQL 字段统一重命名为 'sql'。

        Args:
            db_id: 数据库 ID 过滤 (Exact match)
            fields: 需要保留的字段列表，例如 ["question", "sql"]
            verbose: 是否打印筛选统计信息

        Returns:
            List[Dict]: 处理后的字典列表
        """
        # 1. 预处理：确定需要保留的字段集合
        target_fields = set(fields) if fields else None

        results = []

        for item in self._raw_data:
            # 1. DB_ID 过滤
            if db_id and item.get("db_id") != db_id:
                continue

            new_item = {}
            # 2. 字段映射与提取
            for key, value in item.items():
                # 标准化 Key：如果是 query 或 SQL，统一视为 sql
                normalized_key = "sql" if key in self.SQL_ALIASES else key

                # 判读逻辑：
                # 如果没指定 fields -> 全部保留
                # 如果指定了 fields -> 检查 normalized_key 是否在目标中
                if target_fields is None or normalized_key in target_fields:
                    new_item[normalized_key] = value

            results.append(new_item)

        if verbose:
            logger.info(f"筛选结果: {len(results)}/{len(self._raw_data)} (db_id={db_id})")

        return results

    def get_db_names(self) -> List[str]:
        """获取当前数据集中所有唯一的 db_id，并按字母排序"""
        db_ids = {item.get("db_id") for item in self._raw_data if "db_id" in item}
        return sorted(list(db_ids))

    def inspect_sample(self, index: int = 0):
        """打印特定索引的数据结构，方便调试"""
        if not self._raw_data:
            print("数据为空")
            return

        print(f"--- Sample [{index}] Structure ---")
        try:
            sample = self._raw_data[index]
            print(json.dumps(sample, indent=2, ensure_ascii=False))
        except IndexError:
            print(f"索引 {index} 超出范围")

    # --- Pythonic Magic Methods (让对象更好用) ---

    def __len__(self):
        """允许使用 len(loader)"""
        return len(self._raw_data)

    def __getitem__(self, idx):
        """允许使用 loader[0] 或 loader[0:10]"""
        return self._raw_data[idx]

    def __iter__(self):
        """允许直接使用 for item in loader"""
        return iter(self._raw_data)


if __name__ == '__main__':

    # 1. 初始化
    loader = DataLoader("spider_train")

    # 2. Pythonic 访问
    print(f"数据总条数: {len(loader)}")
    if len(loader) > 0:
        print(f"第一条数据: {loader[0]}")

    # 3. 过滤数据 (注意：它会自动把 'query' 和 'SQL' 都变成 'sql')
    filtered_data = loader.filter(
        db_id="apartment_rentals",
        # fields=["question", "sql"],
        fields=["sql"],
        verbose=True
    )
    print("\n过滤后的数据 (Bank):", filtered_data)

    # 4. 获取所有数据库名
    print("\n包含的数据库:", loader.get_db_names())

    # 5. 查看数据结构
    loader.inspect_sample(0)