from typing import List, Dict, Optional
import json
import re
import sys
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to sys.path
project_root = str(Path(__file__).resolve().parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# Imports
from src.utils.graph_loader import GraphLoader
from src.utils.schema_generator import SchemaGenerator
from src.llm.clients import LLMClient
from src.llm.prompt_manager import PromptManager
from configs.paths import OUTPUT_ROOT


class AnchorSelector:
    """
    锚点选择器 (Anchor Selector)
    负责将自然语言问题映射到数据库中相关的表实体（锚点）。
    """

    def __init__(self, provider: str = "deepseek", model: str = "deepseek-chat"):
        self.prompt_manager = PromptManager()
        # 初始化 LLM 客户端
        self.llm_client = LLMClient(provider=provider, model=model)

        # 预加载 System Prompt
        # 注意：这里假设你的 yaml 文件里 key 还是 'schema_selection_system'
        # 如果你也改了 yaml，请同步修改这里的 key
        self.system_prompt = self.prompt_manager.get_prompt("schema_selection_system")

    def _extract_json(self, text: str) -> Dict:
        """Helper: 从 LLM 响应中提取 JSON"""
        try:
            return json.loads(text)
        except:
            # 尝试提取 Markdown 代码块
            code_block = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
            if code_block:
                return json.loads(code_block.group(1))

            # 尝试提取最外层大括号
            matches = re.search(r'(\{.*\})', text, re.DOTALL)
            if matches:
                return json.loads(matches.group(1))

            raise ValueError("No valid JSON found in response")

    def select_anchors(self, db_schema_str: str, question: str) -> Dict:
        """
        执行锚点选择的核心交互逻辑
        """
        # 获取 User Prompt (自动格式化)
        user_msg = self.prompt_manager.get_prompt(
            "schema_selection_user",
            db_schema=db_schema_str,
            question=question
        )

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_msg}
        ]

        try:
            raw_response = self.llm_client.driver.request(messages)
            return self._extract_json(raw_response)
        except Exception as e:
            logger.error(f"Anchor Selection LLM error: {str(e)}")
            # 返回空结构以防程序崩溃
            return {"selected_entity": [], "reasoning": {}, "decomposition_steps": []}


def run_anchor_selection(dataset_name: str, db_id: str, question: str) -> Dict:
    """
    【核心封装接口】执行一次完整的锚点选择流程。

    Args:
        dataset_name (str): 数据集名称 (如 'spider')
        db_id (str): 数据库 ID (如 'geo')
        question (str): 自然语言问题

    Returns:
        Dict: 包含 'selected_entity', 'reasoning' 等字段的结果
    """
    logger.info(f"Starting Anchor Selection for DB: '{db_id}'")

    # 1. 动态定位 Schema Graph 文件路径
    # 路径逻辑兼容：repo/dataset/db_id/db_id.pkl 或 repo/dataset/db_id.pkl
    base_repo = OUTPUT_ROOT / "schema_graph_repo" / dataset_name
    pkl_path = base_repo / db_id / f"{db_id}.pkl"

    if not pkl_path.exists():
        # 尝试备用路径结构
        pkl_path = base_repo / f"{db_id}.pkl"

    if not pkl_path.exists():
        error_msg = f"Schema Graph file not found at: {pkl_path}"
        logger.error(error_msg)
        return {"error": error_msg, "selected_entity": []}

    try:
        # 2. 加载图结构并生成 Schema 描述
        graph = GraphLoader.load_graph(str(pkl_path))
        if not graph:
            raise ValueError("Graph loaded is empty")

        sg = SchemaGenerator(graph)

        # 将所有表的描述合并为一个字符串
        db_schema_str = "\n".join(
            sg.generate_combined_description(table) for table in sg.tables
        )

        # 3. 初始化选择器并执行
        selector = AnchorSelector()
        result = selector.select_anchors(db_schema_str, question)

        logger.info(f"Anchor Selection completed. Selected: {result.get('selected_entity', [])}")
        return result

    except Exception as e:
        logger.error(f"Fatal error in run_anchor_selection: {e}", exc_info=True)
        return {"error": str(e), "selected_entity": []}


# --- 测试调用示例 ---
if __name__ == "__main__":
    # 模拟外部传入的信息
    test_dataset = "spider"
    test_db = "car_1"
    test_question = "For model volvo, how many cylinders does the car with the least accelerate have?"

    # 调用封装好的函数
    result = run_anchor_selection(test_dataset, test_db, test_question)

    print("\n" + "=" * 30)
    print("锚点选择结果 (Anchor Selection Result):")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("=" * 30)