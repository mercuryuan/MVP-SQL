import os
import sys
import streamlit as st
import networkx as nx
from pathlib import Path
from streamlit_agraph import agraph, Node, Edge, Config

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from configs import paths
from src.utils.dataloder import DataLoader
from src.utils.sql_parser import SQLParser
from src.utils.graph_loader import GraphLoader

# ==========================================
# 0. 全局配置 streamlit run .\src\utils\sql_vis.py
# ==========================================
st.set_page_config(page_title="SQL 可视化分析工具", layout="wide", page_icon="🔍")

# 样式定义 (保持与 vis.py 一致)
STYLE = {
    "Table": {
        "color": "#1976D2",  # 深蓝
        "font_size": 14,
        "size": 50,
        "font_color": "white"
    },
    "Column": {
        "color": "#81C784",  # 浅绿
        "font_size": 10,
        "size": 24,
        "font_color": "black"
    },
    "FOREIGN_KEY": {
        "color": "#E57373",  # 红色
        "width": 3,
        "dashes": True
    },
    "HAS_COLUMN": {
        "color": "#BDBDBD",  # 灰色
        "width": 1.5,
        "dashes": False
    }
}

def smart_truncate(content, length=8):
    """截断显示的 Label"""
    s = str(content)
    if len(s) <= length:
        return s
    return s[:length] + ".."

@st.cache_resource
def load_graph(dataset_name, db_name):
    """加载完整图结构"""
    # 统一处理 dataset_name
    if "spider" in dataset_name.lower():
        dataset_name = "spider"
    elif "bird" in dataset_name.lower():
        dataset_name = "bird"

    # 构造 pkl 路径
    pkl_path = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo", dataset_name, db_name, f"{db_name}.pkl")
    if not os.path.exists(pkl_path):
        return None
    return GraphLoader.load_graph(pkl_path)

@st.cache_resource
def get_sql_parser(dataset_name, db_name):
    """获取缓存的 SQL 解析器实例"""
    return SQLParser(dataset_name, db_name)

def extract_subgraph(G, entities):
    """
    根据 SQL 解析出的实体提取子图。
    entities: {table_name: [col_name, ...]}
    """
    if G is None:
        return None
    
    subgraph_nodes = set()
    
    # 1. 添加涉及的表节点和列节点
    for table, columns in entities.items():
        # 添加表节点
        if G.has_node(table):
            subgraph_nodes.add(table)
        
        # 添加列节点
        for col in columns:
            # Column node id format: "table.col"
            col_node_id = f"{table}.{col}"
            if G.has_node(col_node_id):
                subgraph_nodes.add(col_node_id)
                
    # 2. 构建子图
    # 使用 subgraph 方法会保留所有连接这些节点的边
    # 但我们可能只想保留特定的边：
    # - HAS_COLUMN: table -> col (必须都在 subgraph_nodes 里)
    # - FOREIGN_KEY: table -> table (必须都在 subgraph_nodes 里)
    
    sub_G = G.subgraph(list(subgraph_nodes)).copy()
    
    return sub_G

def convert_nx_to_agraph(G):
    """将 NetworkX 图转换为 agraph 组件需要的格式"""
    nodes = []
    edges = []
    
    if G is None:
        return [], []

    for node_id, attrs in G.nodes(data=True):
        node_type = attrs.get("type", "Unknown")
        conf = STYLE.get(node_type, {})
        real_name = attrs.get("name", node_id)
        
        truncate_len = 8 if node_type == "Column" else 10
        label_text = smart_truncate(real_name, truncate_len)
        diameter = conf.get("size", 30)

        nodes.append(Node(
            id=node_id,
            label=label_text,
            shape="ellipse",
            widthConstraint={"minimum": diameter, "maximum": diameter},
            heightConstraint={"minimum": diameter, "maximum": diameter},
            color=conf.get("color"),
            font={
                "color": conf.get("font_color"),
                "size": conf.get("font_size"),
                "face": "arial"
            },
            title=f"Name: {real_name}\nType: {node_type}",
            borderWidth=1,
            shadow={"enabled": True, "color": "rgba(0,0,0,0.3)", "size": 5, "x": 2, "y": 2}
        ))

    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get("type")
        conf = STYLE.get(edge_type, {})
        
        edges.append(Edge(
            source=u,
            target=v,
            color=conf.get("color"),
            width=conf.get("width"),
            dashes=conf.get("dashes", False),
            arrows={"to": {"enabled": True, "scaleFactor": 0.8}},
            label=edge_type if edge_type == "FOREIGN_KEY" else "" # 仅外键显示标签
        ))

    return nodes, edges

# ==========================================
# 1. 侧边栏与数据加载
# ==========================================
st.sidebar.title("🗄️ 数据集选择")

dataset_options = ["spider", "spider_dev", "bird", "bird_dev"]
selected_dataset = st.sidebar.selectbox("选择数据集", dataset_options, index=0)

# 加载数据
try:
    loader = DataLoader(selected_dataset)
    db_list = loader.list_dbnames()
    
    selected_db = st.sidebar.selectbox("选择数据库", db_list)
    
    # 筛选当前数据库下的所有问题
    db_data = loader.filter_data(db_id=selected_db, fields=["question", "sql_query", "evidence"])
    
    # 构建问题列表供选择
    # 仅使用最简单的 selectbox，不进行复杂的双向绑定
    question_options = [f"{i}: {item['question']}" for i, item in enumerate(db_data)]
    
    # 格式化函数，用于侧边栏显示
    def format_func(idx):
        return question_options[idx][:40] + "..."

    # 简单的选择框
    selected_q_idx = st.sidebar.selectbox(
        "选择问题", 
        range(len(db_data)), 
        format_func=format_func,
        index=0
    )
    
    current_item = db_data[selected_q_idx]

except Exception as e:
    st.error(f"数据加载失败: {e}")
    st.stop()

# ==========================================
# 2. 主界面
# ==========================================
st.title("SQL 解析与可视化分析")

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📋 数据详情")
    st.markdown(f"**Question:** {current_item['question']}")
    st.code(current_item['sql_query'], language="sql")
    
    if current_item.get('evidence'):
        st.info(f"**Evidence:** {current_item['evidence']}")
    else:
        st.caption("No evidence provided.")

    st.subheader("📊 SQL 解析报告")
    
    # 解析 SQL
    try:
        parser = get_sql_parser(selected_dataset, selected_db)
        
        # 生成文本报告
        report = parser.generate_report(current_item['sql_query'])
        st.text(report)
        
        # 获取结构化实体用于提取子图
        entities = parser.extract_entities(current_item['sql_query'])
        
    except Exception as e:
        st.error(f"解析失败: {e}")
        entities = {}

with col2:
    st.subheader("🕸️ 子图可视化")
    
    if entities:
        # 加载完整图
        full_graph = load_graph(selected_dataset, selected_db)
        
        if full_graph:
            # 提取子图
            sub_graph = extract_subgraph(full_graph, entities)
            
            if sub_graph and sub_graph.number_of_nodes() > 0:
                nodes, edges = convert_nx_to_agraph(sub_graph)
                
                config = Config(
                    width=600,
                    height=600,
                    directed=True, 
                    physics=True, 
                    hierarchical=False,
                    nodeHighlightBehavior=True,
                    highlightColor="#F7A7A6",
                    collapsible=False
                )
                
                agraph(nodes=nodes, edges=edges, config=config)
            else:
                st.warning("提取的子图为空 (可能是解析出的实体在图中未找到)")
        else:
            st.warning(f"未找到数据库 {selected_db} 的图结构文件 (.pkl)。请先运行 SchemaPipeline 生成。")
    else:
        st.info("等待解析成功后显示子图...")
