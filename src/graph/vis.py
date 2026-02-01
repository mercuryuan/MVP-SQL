import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
import networkx as nx
import os
import json
import pandas as pd
import pickle
from configs import paths
from src.utils.graph_loader import GraphLoader

# ==========================================
# 0. 全局配置
# ==========================================
st.set_page_config(page_title="Schema 图结构可视化", layout="wide", page_icon="🕸️")

# 【请修改此处】图数据存储的根目录
# ROOT_DIR = paths.OUTPUT_ROOT
ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "converted_graph_pkl")

# 样式定义
# 【修改点1】调整大小定义，这里的 size 现在代表直径
STYLE = {
    "Table": {
        "color": "#1976D2",  # 深蓝
        "font_size": 14,  # 适中字体
        "size": 50,  # 直径 (变大以容纳文字)
        "font_color": "white"
    },
    "Column": {
        "color": "#81C784",  # 浅绿
        "font_size": 10,  # 小字体
        "size": 24,  # 直径
        "font_color": "black"
    },
    "FOREIGN_KEY": {
        "color": "#E57373",  # 红色
        "width": 3,  # 加粗
        "dashes": True
    },
    "HAS_COLUMN": {
        "color": "#BDBDBD",  # 灰色
        "width": 1.5,
        "dashes": False
    }
}


# ==========================================
# 1. 工具函数
# ==========================================
def get_subdirs(path):
    if not os.path.exists(path):
        return []
    return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])


def smart_truncate(content, length=8):
    """截断显示的 Label"""
    s = str(content)
    if len(s) <= length:
        return s
    return s[:length] + ".."


@st.cache_data
def load_graph_from_pkl(pkl_path):
    G = GraphLoader.load_graph(pkl_path)
    if G is None:
        st.error(f"文件加载失败，请检查日志。路径: {pkl_path}")
    return G


# ==========================================
# 2. 侧边栏逻辑
# ==========================================
def render_sidebar():
    selected_file = None
    with st.sidebar:
        st.header("🗄️ 数据库选择")

        datasets = get_subdirs(ROOT_DIR)
        if not datasets:
            st.warning(f"根目录 {ROOT_DIR} 为空")
            return None, True

        selected_dataset = st.selectbox("数据集", datasets)

        dataset_path = os.path.join(ROOT_DIR, selected_dataset)
        databases = get_subdirs(dataset_path)

        if not databases:
            return None, True

        selected_db = st.selectbox("数据库", databases)

        # 自动查找 .pkl
        db_path = os.path.join(dataset_path, selected_db)
        if os.path.exists(db_path):
            for file in os.listdir(db_path):
                if file.endswith(".pkl"):
                    selected_file = os.path.join(db_path, file)
                    break

        if selected_file:
            st.caption(f"已加载: {os.path.basename(selected_file)}")
        else:
            st.error("未找到 .pkl 文件")

        st.markdown("---")
        show_columns = st.checkbox("显示列节点 (Show Columns)", value=True)

    return selected_file, show_columns


# ==========================================
# 3. 图转换逻辑 (【修改点】支持美观圆形和边ID)
# ==========================================
def convert_nx_to_agraph(G, show_columns):
    nodes = []
    edges = []
    # 【修改点2】新增 edge_map 用于存储边数据以便点击时查询
    edge_map = {}

    for node_id, attrs in G.nodes(data=True):
        node_type = attrs.get("type", "Unknown")

        if not show_columns and node_type == "Column":
            continue

        conf = STYLE.get(node_type, {})
        real_name = attrs.get("name", node_id)

        # 截断长度根据节点类型区分
        truncate_len = 8 if node_type == "Column" else 10
        label_text = smart_truncate(real_name, truncate_len)

        # 获取直径尺寸
        diameter = conf.get("size", 30)

        nodes.append(Node(
            id=node_id,
            label=label_text,
            # 【修改点1】使用 ellipse 配合严格的宽高约束来实现“文字在内的完美圆形”
            shape="ellipse",
            widthConstraint={"minimum": diameter, "maximum": diameter},
            heightConstraint={"minimum": diameter, "maximum": diameter},
            color=conf.get("color"),
            font={
                "color": conf.get("font_color"),
                "size": conf.get("font_size"),
                "face": "arial"
            },
            title=f"Name: {real_name}\nType: {node_type}",  # Tooltip
            borderWidth=1,
            borderWidthSelected=3,
            # 添加阴影增加立体感，稍微美化一下
            shadow={"enabled": True, "color": "rgba(0,0,0,0.3)", "size": 5, "x": 2, "y": 2}
        ))

    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get("type")

        if not show_columns:
            if edge_type == "HAS_COLUMN": continue
            if G.nodes[u].get("type") == "Column" or G.nodes[v].get("type") == "Column": continue

        conf = STYLE.get(edge_type, {})

        # 【修改点2】生成唯一的边 ID
        edge_id = f"{u}___{v}___{edge_type}"
        # 存储边属性映射
        edge_map[edge_id] = attrs

        edges.append(Edge(
            id=edge_id,  # 设置 ID
            source=u,
            target=v,
            color=conf.get("color"),
            width=conf.get("width"),
            dashes=conf.get("dashes", False),
            # 增加箭头大小
            arrows={"to": {"enabled": True, "scaleFactor": 0.8}}
        ))

    # 【修改点2】返回 nodes, edges 和 edge_map
    return nodes, edges, edge_map


# ==========================================
# 4. 详情面板 (【修改点】支持边点击展示)
# ==========================================
def render_details_panel(G, edge_map, selected_id):
    st.subheader("📝 属性面板")

    if not selected_id:
        st.info("👈 选择节点或关系查看详情")
        return

    # --- 情况 A: 点击的是节点 ---
    if G.has_node(selected_id):
        data = G.nodes[selected_id]
        node_type = data.get('type', 'N/A')
        node_name = data.get('name', selected_id)
        st.write(data)

        # 1. 顶部卡片
        bg_color = STYLE.get(node_type, {}).get('color', '#555')
        st.markdown(f"""
        <div style="padding:12px; border-radius:6px; background-color:{bg_color}; color:white; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
            <h3 style="margin:0; font-size: 20px; font-family: monospace;">{node_name}</h3>
            <div style="margin-top:4px; font-size: 12px; opacity: 0.9; text-transform: uppercase; letter-spacing: 1px;">{node_type} Node</div>
        </div>
        """, unsafe_allow_html=True)

        # 2. 统计信息列表 (复用 HTML Table 逻辑)
        _render_compact_table(data, ignore_keys={'type', 'name', 'samples', 'word_frequency', 'columns', 'foreign_key',
                                                 'reference_to', 'referenced_by', 'referenced_to', 'id'})

        # 3. 采样数据 & 词频 & 结构 (保持不变)
        if "samples" in data and data["samples"]:
            st.markdown("---")
            st.markdown("**🎲 采样数据**")
            df_samples = pd.DataFrame(data["samples"], columns=["Values"])
            st.dataframe(df_samples, height=150, hide_index=True, use_container_width=True)

        if "word_frequency" in data:
            wf = data["word_frequency"]
            if isinstance(wf, str):
                try:
                    wf = json.loads(wf)
                except:
                    wf = {}
            if wf and isinstance(wf, dict):
                st.markdown("---")
                st.markdown("**🔡 高频词汇**")
                df_wf = pd.DataFrame(list(wf.items()), columns=["Word", "Freq"])
                df_wf = df_wf.sort_values(by="Freq", ascending=False).head(10)
                st.dataframe(df_wf, height=150, hide_index=True, use_container_width=True)

        if node_type == "Table" and "columns" in data:
            st.markdown("---")
            with st.expander(f"包含列 ({len(data['columns'])})", expanded=False):
                st.write(", ".join(data['columns']))

    # --- 【修改点3】情况 B: 点击的是边 ---
    elif selected_id in edge_map:
        data = edge_map[selected_id]
        edge_type = data.get('type', 'Relation')

        # 1. 顶部卡片 (边的样式)
        bg_color = STYLE.get(edge_type, {}).get('color', '#999')
        st.markdown(f"""
        <div style="padding:12px; border-radius:6px; background-color:{bg_color}; color:white; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
            <h3 style="margin:0; font-size: 18px; font-family: monospace;">Relationship</h3>
            <div style="margin-top:4px; font-size: 12px; opacity: 0.9; text-transform: uppercase; letter-spacing: 1px;">{edge_type}</div>
        </div>
        """, unsafe_allow_html=True)

        # 2. 关系属性列表
        # 展示所有属性，除了 type
        _render_compact_table(data, ignore_keys={'type'})

    else:
        st.warning(f"未找到 ID 为 {selected_id} 的元素信息")


def _render_compact_table(data, ignore_keys):
    """辅助函数：渲染紧凑的 HTML 属性表"""
    simple_stats = {}
    # 强制优先显示的属性
    priority_keys = ['data_type', 'row_count', 'from_table', 'from_column', 'to_table', 'to_column', 'relation_type']

    for k in priority_keys:
        if k in data:
            simple_stats[k] = data[k]

    for k, v in data.items():
        if k not in ignore_keys and k not in priority_keys and isinstance(v, (str, int, float, bool, type(None))):
            simple_stats[k] = v

    if simple_stats:
        st.markdown("**📋 属性列表**")
        table_html = """
        <style>
            .prop-table { width: 100%; border-collapse: collapse; font-size: 13px; font-family: sans-serif; }
            .prop-table td { padding: 5px 8px; border-bottom: 1px solid #eee; vertical-align: top;}
            .prop-key { color: #555; font-weight: 600; width: 40%; white-space: nowrap; }
            .prop-val { color: #222; font-family: monospace; word-break: break-all; }
        </style>
        <table class="prop-table">
        """
        for k, v in simple_stats.items():
            display_v = v
            if isinstance(v, float): display_v = f"{v:.2f}"
            table_html += f"<tr><td class='prop-key'>{k}</td><td class='prop-val'>{display_v}</td></tr>"
        table_html += "</table>"
        st.markdown(table_html, unsafe_allow_html=True)


# ==========================================
# 5. 主程序
# ==========================================
def main():
    pkl_file, show_columns = render_sidebar()

    if not pkl_file:
        st.info("👈 请在左侧选择数据以开始")
        st.stop()

    G = load_graph_from_pkl(pkl_file)
    if G is None: st.stop()

    col_graph, col_details = st.columns([3, 1])

    with col_graph:
        # 【修改点2】接收 edge_map
        nodes, edges, edge_map = convert_nx_to_agraph(G, show_columns)

        config = Config(
            width="100%",
            height=850,
            directed=True,
            physics=True,
            hierarchical=False,
            physicsOptions={
                "barnesHut": {
                    "gravitationalConstant": -5000,
                    "springLength": 220,
                    "springConstant": 0.05,
                    "damping": 0.09
                }
            }
        )

        # 获取点击的 ID (可能是节点 ID，也可能是边 ID)
        selected_id = agraph(nodes=nodes, edges=edges, config=config)

    with col_details:
        # 【修改点3】传递 edge_map 和 selected_id
        render_details_panel(G, edge_map, selected_id)


if __name__ == "__main__":
    main()
    # 命令为 streamlit run src/graph/vis.py
