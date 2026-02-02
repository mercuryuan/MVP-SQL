import pickle
import os
import networkx as nx
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GraphLoader:
    """
    图结构加载工具类。
    用于从 pickle 文件加载 NetworkX 图对象。
    """

    @staticmethod
    def load_graph(pkl_path: str) -> nx.DiGraph:
        """
        从 pickle 文件加载图结构。

        Args:
            pkl_path (str): pickle 文件的路径。

        Returns:
            nx.DiGraph: 加载的 NetworkX 图对象。如果加载失败，返回 None。
        """
        if not os.path.exists(pkl_path):
            logger.error(f"文件不存在: {pkl_path}")
            return None

        try:
            with open(pkl_path, "rb") as f:
                G = pickle.load(f)

            # Basic validation
            if not isinstance(G, (nx.DiGraph, nx.Graph)):
                logger.warning(f"加载的对象类型不是 NetworkX Graph/DiGraph，而是: {type(G)}")

            logger.info(f"成功加载图结构: {pkl_path}, 节点数: {G.number_of_nodes()}, 边数: {G.number_of_edges()}")
            return G
        except Exception as e:
            logger.error(f"文件加载失败: {pkl_path}, 错误: {e}")
            return None
