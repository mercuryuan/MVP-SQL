from typing import List, Dict, Optional
import json
import re
import sys
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目根目录到 sys.path
project_root = str(Path(__file__).resolve().parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# 导入项目模块
from src.utils.graph_loader import GraphLoader
from src.utils.schema_generator import SchemaGenerator
from src.llm.clients import LLMClient
from src.llm.prompt_manager import PromptManager
from src.schema_linking.iterative_expander import IterativeSubgraphExpander
from configs.paths import OUTPUT_ROOT


import networkx as nx
from networkx.algorithms.approximation import steiner_tree

class SubgraphRouter:
    """
    智能子图路由器 (Smart Subgraph Router)
    基于锚点数量和图拓扑结构，决定 Schema Linking 的下一步策略。
    实现 Neuro-Symbolic 思想：LLM 找点，图算法铺路。
    """
    
    def __init__(self, graph: nx.DiGraph):
        # 计算连通性时使用无向视图，因为 SQL JOIN 是无向的
        self.G_undirected = graph.to_undirected()
        self.original_G = graph
        
    def route(self, anchor_tables: List[str]) -> Dict:
        """
        核心路由逻辑
        Args:
            anchor_tables: 初始选定的锚点表名列表
        Returns:
            Dict: 包含 status, subgraph_nodes, message 等字段
        """
        # 过滤掉不在图中的表
        valid_anchors = [t for t in anchor_tables if t in self.G_undirected]
        n = len(valid_anchors)
        
        # 🌿 分支 0: 无锚点
        if n == 0:
            return {"status": "failed", "reason": "No valid initial tables found."}
            
        # 🌿 分支 1: 单表直通
        if n == 1:
            return {
                "status": "fast_track",
                "subgraph_nodes": valid_anchors,
                "message": "Single table query.",
                "branch": "1-SingleTable"
            }
            
        # 🌿 分支 2: 双表寻路
        if n == 2:
            return self._handle_two_tables(valid_anchors[0], valid_anchors[1])
            
        # 🌿 分支 3: 多表斯坦纳树
        return self._handle_multi_tables(valid_anchors)

    def _handle_two_tables(self, t1: str, t2: str) -> Dict:
        try:
            # 获取所有最短路径
            paths = list(nx.all_shortest_paths(self.G_undirected, source=t1, target=t2))
            
            # 子分支 2.A: 唯一通途
            if len(paths) == 1:
                return {
                    "status": "fast_track",
                    "subgraph_nodes": paths[0],
                    "message": "Unique shortest path found.",
                    "branch": "2A-UniquePath"
                }
                
            # 子分支 2.B: 歧义探索
            return {
                "status": "ambiguity_needs_resolution",
                "anchors": [t1, t2],
                "path_clues": paths, # 返回所有可能的路径供 LLM 决策
                "message": f"Found {len(paths)} shortest paths.",
                "branch": "2B-Ambiguity"
            }
            
        except nx.NetworkXNoPath:
            return {"status": "failed", "reason": "Nodes are physically disconnected."}

    def _handle_multi_tables(self, anchors: List[str]) -> Dict:
        try:
            # 近似斯坦纳树
            # 注意: steiner_tree 要求图是连通的，或者终端节点在同一连通分量
            # 这里简单处理，如果不在同一连通分量会抛出异常
            tree = steiner_tree(self.G_undirected, anchors)
            tree_nodes = list(tree.nodes())
            
            # 毒化防御机制
            extra_nodes_count = len(tree_nodes) - len(anchors)
            
            # 子分支 3.A: 结构紧凑
            if extra_nodes_count <= 3: # 阈值可配置
                return {
                    "status": "fast_track",
                    "subgraph_nodes": tree_nodes,
                    "message": "Steiner tree connected safely.",
                    "branch": "3A-CompactTree"
                }
                
            # 子分支 3.B: 过度延伸 (毒化)
            return {
                "status": "toxic_anchors_detected",
                "anchors": anchors,
                "tree_nodes": tree_nodes,
                "message": f"Suspected poisoning. Introduced {extra_nodes_count} extra nodes.",
                "branch": "3B-ToxicAlert"
            }
            
        except Exception as e:
            # 可能是节点不连通等原因
            return {"status": "failed", "reason": str(e)}

class AnchorSelector:
    """
    锚点选择器 (Anchor Selector)
    负责将自然语言问题映射到数据库中相关的表实体（锚点）。
    """

    def __init__(self, provider: str = "deepseek", model: Optional[str] = None):
        """
        初始化选择器
        :param provider: 模型供应商 (openai, deepseek, gemini, ollama)
        :param model: 具体模型名称 (如 gpt-4o, deepseek-chat, gemini-2.0-flash)。
                      如果为 None，则使用 LLMClient 内部定义的默认值。
        """
        self.prompt_manager = PromptManager()
        self.prompt_manager.reload() # Force reload prompts to ensure latest YAMLs are loaded

        # 初始化 LLM 客户端，透传参数
        self.llm_client = LLMClient(provider=provider, model=model)

        # 预加载 System Prompt
        self.system_prompt = self.prompt_manager.get_prompt("schema_selection_system")

    def _extract_json(self, text: str) -> Dict:
        """从 LLM 响应中提取 JSON"""
        # 1. 尝试直接完整解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2. 尝试提取 Markdown 代码块 (优先级高)
        code_block = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
        if code_block:
            try:
                return json.loads(code_block.group(1))
            except json.JSONDecodeError:
                pass

        # 3. 尝试寻找第一个有效 JSON 对象 (解决 "Extra data" 问题的核心)
        start_idx = text.find('{')
        if start_idx != -1:
            try:
                # raw_decode 会在解析完第一个对象后停止，忽略后续文本
                obj, _ = json.JSONDecoder().raw_decode(text[start_idx:])
                return obj
            except json.JSONDecodeError:
                pass

        # 4. 最后尝试贪婪匹配作为兜底
        matches = re.search(r'(\{.*\})', text, re.DOTALL)
        if matches:
            try:
                return json.loads(matches.group(1))
            except json.JSONDecodeError:
                pass

        # 如果都失败，记录错误但不要抛出异常中断流程，而是返回空结果
        logger.error(f"JSON extraction failed for text: {text[:200]}...")
        return {
            "selected_entity": [],
            "reasoning": {},
            "question_decomposition_steps": []
        }

    def select_anchors(self, db_schema_str: str, question: str) -> Dict:
        """执行锚点选择的核心交互逻辑"""
        # 获取 User Prompt
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
            result = self._extract_json(raw_response)
            
            # Add prompts to result for visualization
            result["_prompts"] = {
                "system": self.system_prompt,
                "user": user_msg
            }
            return result
        except Exception as e:
            logger.error(f"Anchor Selection LLM error: {str(e)}")
            return {"selected_entity": [], "reasoning": {}, "question_decomposition_steps": []}

def initialize_subgraph(dataset_name: str, db_id: str, question: str, provider: str = "deepseek", model: Optional[str] = None, schema_detail_level: str = "full", run_sl2: bool = True) -> Dict:
    """
    【新版核心接口】执行完整的子图初始化流程 (Smart Initialization Phase)
    Step 1: LLM Anchor Selection
    Step 2: Graph Algorithm Routing & Decision Tree
    Step 3: Iterative Expansion (Optional)
    """
    # 1. 执行 Step 1: 锚点选择 (复用原有 run_anchor_selection 逻辑的一部分，但需要图对象)
    # 为了避免重复加载图，这里我们稍微重构一下流程
    
    # 动态定位 Schema Graph 文件路径
    base_repo = OUTPUT_ROOT / "schema_graph_repo" / dataset_name
    pkl_path = base_repo / db_id / f"{db_id}.pkl"
    if not pkl_path.exists(): pkl_path = base_repo / f"{db_id}.pkl"
    
    if not pkl_path.exists():
        return {"error": f"Graph file not found: {pkl_path}"}
        
    try:
        # 加载图
        graph = GraphLoader.load_graph(str(pkl_path))
        if not graph: raise ValueError("Graph empty")
        
        # 生成 Schema 描述
        sg = SchemaGenerator(graph)
        db_schema_str = "\n".join(sg.generate_combined_description(table, detail_level=schema_detail_level) for table in sg.tables)
        
        # LLM 选择锚点
        selector = AnchorSelector(provider=provider, model=model)
        llm_result = selector.select_anchors(db_schema_str, question)
        
        initial_anchors = llm_result.get("selected_entity", [])
        # 过滤只保留表名 (去除列名)
        table_anchors = list(set([a.split('.')[0] for a in initial_anchors]))
        
        # Step 2: 核心决策路由
        router = SubgraphRouter(graph)
        route_result = router.route(table_anchors)
        
        # 合并结果
        final_result = {
            "step1_llm_result": llm_result,
            "step2_route_result": route_result,
            "final_subgraph_nodes": route_result.get("subgraph_nodes", []), # 可能是空，取决于 status
            "status": route_result.get("status", "unknown")
        }
        
        # 如果需要解决歧义 (Ambiguity Resolution)，这里可以再次调用 LLM (Future Work)
        if route_result["status"] == "ambiguity_needs_resolution":
             # TODO: Implement ambiguity resolution prompt
             pass

        # Step 3: Iterative Expansion (SL2)
        # Execute only if we have a valid starting subgraph (Single Table or Connected Tree)
        # AND if run_sl2 is True
        if run_sl2 and route_result["status"] in ["fast_track", "ambiguity_needs_resolution", "3A-CompactTree"]:
            try:
                # Use the nodes from routing as the initial core for expansion
                # Ensure we only pass table names, not columns (though currently route returns tables)
                initial_tables_sl2 = [n for n in final_result["final_subgraph_nodes"] if graph.nodes[n].get("type", "Table") == "Table"]
                
                if initial_tables_sl2:
                    logger.info(f"Starting SL2 Iterative Expansion with core: {initial_tables_sl2}")
                    expander = IterativeSubgraphExpander(graph, provider=provider, model=model)
                    sl2_result = expander.run_expansion(question, initial_tables_sl2)
                    
                    final_result["step3_expansion_result"] = sl2_result
                    # Update final nodes with SL2 result (which includes columns)
                    final_result["final_subgraph_nodes"] = sl2_result["final_subgraph_nodes"]
                    final_result["status"] = "sl2_completed"
                else:
                    logger.warning("No valid tables found for SL2 expansion.")
            except Exception as e:
                logger.error(f"SL2 Expansion failed: {e}", exc_info=True)
                final_result["sl2_error"] = str(e)
             
        return final_result
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}", exc_info=True)
        return {"error": str(e)}

# 保持兼容性，重命名旧的 run_anchor_selection 或保留它但标记为 Deprecated
# 这里我们更新 run_anchor_selection 让它直接调用新逻辑，或者让它只做 Step 1
# 为了满足用户需求 "完成相关的逻辑实现"，我们应该使用新的 initialize_subgraph 替代原来的单一 LLM 调用

def run_anchor_selection(
        dataset_name: str,
        db_id: str,
        question: str,
        provider: str = "deepseek",
        model: Optional[str] = None,
        schema_detail_level: str = "full",
        run_sl2: bool = True
) -> Dict:
    """
    [Updated] 现在调用完整的 initialize_subgraph 流程
    """
    return initialize_subgraph(dataset_name, db_id, question, provider, model, schema_detail_level, run_sl2)



# --- 测试调用示例 ---
if __name__ == "__main__":
    # 模拟外部调用
    test_dataset = "spider"
    test_db = "academic"
    test_question = """return me the number of the keywords related to " H. V. Jagadish " ."""

    print("\n--- Test 1: Using Default (DeepSeek) ---")
    res1 = run_anchor_selection(test_dataset, test_db, test_question)
    print(f"Result (DeepSeek): {res1.get('selected_entity')}")

    print("\n--- Test 2: Using Gemini (Explicit) ---")
    # 这里显式传入 provider 和 model
    res2 = run_anchor_selection(
        test_dataset,
        test_db,
        test_question,
        provider="ollama",
        model="llama3.2:3b"
    )
    print(f"Result (Gemini): {res2.get('selected_entity')}")