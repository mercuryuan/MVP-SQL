import json
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from configs import paths
from src.utils.graph_loader import GraphLoader
from src.utils.graph_explorer import GraphExplorer
from src.utils.validator import Validator

# ==========================================
# 0. 全局配置
# ==========================================
st.set_page_config(page_title="Schema 图结构可视化", layout="wide", page_icon="🕸️")

# 【请修改此处】图数据存储的根目录
# ROOT_DIR = paths.OUTPUT_ROOT
ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")

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
# 5. 验证器测试模块
# ==========================================
def render_validator_sandbox(G):
    def nested_columns_to_dict(nested):
        selected_columns = {}
        if isinstance(nested, list):
            for row in nested:
                if isinstance(row, list) and len(row) == 2:
                    table = str(row[0]).strip()
                    cols_raw = row[1] if isinstance(row[1], list) else [row[1]]
                    cols = []
                    for c in cols_raw:
                        col = str(c).strip()
                        if col and col not in cols:
                            cols.append(col)
                    if table:
                        selected_columns[table] = cols
        return selected_columns

    def nested_paths_to_dict(nested):
        selected_reference_path = {}
        if isinstance(nested, list):
            for row in nested:
                if isinstance(row, list) and len(row) >= 1:
                    path = str(row[0]).strip()
                    reason = str(row[1]).strip() if len(row) > 1 else ""
                    if path:
                        selected_reference_path[path] = reason
        return selected_reference_path

    def selected_columns_to_nested(selected_columns):
        if not isinstance(selected_columns, dict):
            return []
        nested = []
        for table, cols in selected_columns.items():
            columns = cols if isinstance(cols, list) else [cols]
            nested.append([str(table), [str(c) for c in columns]])
        return nested

    def selected_paths_to_nested(selected_paths):
        if not isinstance(selected_paths, dict):
            return []
        return [[str(path), str(reason)] for path, reason in selected_paths.items()]

    def collect_column_entities(selected_columns):
        entities = set()
        if not isinstance(selected_columns, dict):
            return entities
        for table, cols in selected_columns.items():
            if isinstance(cols, list):
                for col in cols:
                    entities.add(f"{table}.{col}")
        return entities

    st.markdown("---")
    st.header("🧪 Validator 全功能实验台")
    st.info("输入 LLM 原始 JSON，展示：格式归一化、严格校验、清洗结果、失败原因、差异对比、过滤接口结果。")

    explorer = GraphExplorer(G)
    validator = Validator(explorer)
    first_two_tables_nested = explorer.get_first_n_tables_schema_nested(2)
    fk_tables_nested, fk_paths_dict = explorer.get_any_foreign_key_nested()
    fk_paths_nested = [[k, v] for k, v in fk_paths_dict.items()]

    invalid_case_nested = json.loads(json.dumps(first_two_tables_nested, ensure_ascii=False))
    if invalid_case_nested and len(invalid_case_nested) > 0:
        invalid_case_nested[0][1].append("__non_exist_col__")

    presets = {
        "Schema 基线（前两表）": {
            "selected_columns_nested": first_two_tables_nested,
            "selected_reference_path_nested": [],
            "reasoning": {},
            "to_solve_the_question": {"is_solvable": True}
        },
        "FK 基线（任一外键）": {
            "selected_columns_nested": fk_tables_nested,
            "selected_reference_path_nested": fk_paths_nested,
            "reasoning": {},
            "to_solve_the_question": {"is_solvable": True}
        },
        "错误注入（含不存在列）": {
            "selected_columns_nested": invalid_case_nested,
            "selected_reference_path_nested": fk_paths_nested,
            "reasoning": {},
            "to_solve_the_question": {"is_solvable": True}
        }
    }

    preset_name = st.selectbox("模板", list(presets.keys()))
    default_json = json.dumps(presets[preset_name], ensure_ascii=False, indent=2)
    json_input = st.text_area("LLM 原始输出 JSON", value=default_json, height=420)
    run_btn = st.button("🚀 执行验证器全流程", type="primary", use_container_width=True)

    if not run_btn:
        return

    try:
        import copy
        raw_input = json.loads(json_input)

        prepared_input = copy.deepcopy(raw_input)
        if "selected_columns_nested" in prepared_input:
            prepared_input["selected_columns"] = nested_columns_to_dict(prepared_input.get("selected_columns_nested", []))
        if "selected_reference_path_nested" in prepared_input:
            prepared_input["selected_reference_path"] = nested_paths_to_dict(prepared_input.get("selected_reference_path_nested", []))

        st.subheader("1) 输入结构诊断")
        schema_diag = {
            "selected_columns_type": type(prepared_input.get("selected_columns")).__name__,
            "selected_reference_path_type": type(prepared_input.get("selected_reference_path")).__name__,
            "reasoning_type": type(prepared_input.get("reasoning")).__name__ if "reasoning" in prepared_input else "missing",
            "selected_columns_count": len(prepared_input.get("selected_columns", {})) if isinstance(prepared_input.get("selected_columns"), dict) else 0,
            "selected_paths_count": len(prepared_input.get("selected_reference_path", {})) if isinstance(prepared_input.get("selected_reference_path"), dict) else 0
        }
        st.json(schema_diag)

        if "selected_entity" in prepared_input:
            st.subheader("2) SL1 验证")
            sl1_valid, sl1_result, sl1_msg = validator.validate_sl1(copy.deepcopy(prepared_input))
            status_col1, status_col2, status_col3 = st.columns(3)
            status_col1.metric("SL1 合法", "是" if sl1_valid else "否")
            status_col2.metric("输入表数量", len(prepared_input.get("selected_entity", [])) if isinstance(prepared_input.get("selected_entity"), list) else 0)
            status_col3.metric("输出表数量", len(sl1_result.get("selected_entity", [])) if isinstance(sl1_result.get("selected_entity"), list) else 0)
            st.write(sl1_msg if sl1_msg else "无附加信息")
            view_a, view_b = st.columns(2)
            with view_a:
                st.markdown("**SL1 输入**")
                st.json(prepared_input)
            with view_b:
                st.markdown("**SL1 输出**")
                st.json(sl1_result)
            return

        st.subheader("2) Validator 主流程 (SL2/SL3)")
        original_for_compare = copy.deepcopy(prepared_input)
        validated_result = validator.validate_and_correct(copy.deepcopy(prepared_input))
        to_solve = validated_result.get("to_solve_the_question", {})
        failure_reasons = to_solve.get("failure_reasons", []) if isinstance(to_solve, dict) else []
        is_solvable = to_solve.get("is_solvable") if isinstance(to_solve, dict) else None

        metric1, metric2, metric3, metric4 = st.columns(4)
        metric1.metric("is_solvable", str(is_solvable))
        metric2.metric("失败条数", len(failure_reasons))
        metric3.metric(
            "输入列实体数",
            len(collect_column_entities(original_for_compare.get("selected_columns", {})))
        )
        metric4.metric(
            "输出列实体数",
            len(collect_column_entities(validated_result.get("selected_columns", {})))
        )

        if failure_reasons:
            st.error("本轮存在校验失败项，已清洗非法实体并返回 failure_reasons。")
        else:
            st.success("本轮无失败项，输出已通过验证并规范化。")

        st.subheader("3) 原始 vs 验证后")
        left, right = st.columns(2)
        with left:
            st.markdown("**LLM 原始输入（用于验证前）**")
            st.json(original_for_compare)
        with right:
            st.markdown("**验证器输出（清洗后）**")
            st.json(validated_result)

        st.subheader("4) 嵌套列表视角")
        nested_view_a, nested_view_b = st.columns(2)
        with nested_view_a:
            st.markdown("**输入嵌套结构**")
            st.json({
                "selected_columns_nested": selected_columns_to_nested(original_for_compare.get("selected_columns", {})),
                "selected_reference_path_nested": selected_paths_to_nested(original_for_compare.get("selected_reference_path", {}))
            })
        with nested_view_b:
            st.markdown("**输出嵌套结构**")
            st.json({
                "selected_columns_nested": selected_columns_to_nested(validated_result.get("selected_columns", {})),
                "selected_reference_path_nested": selected_paths_to_nested(validated_result.get("selected_reference_path", {}))
            })

        st.subheader("5) 差异摘要")
        in_tables = set(original_for_compare.get("selected_columns", {}).keys()) if isinstance(original_for_compare.get("selected_columns"), dict) else set()
        out_tables = set(validated_result.get("selected_columns", {}).keys()) if isinstance(validated_result.get("selected_columns"), dict) else set()
        in_columns = collect_column_entities(original_for_compare.get("selected_columns", {}))
        out_columns = collect_column_entities(validated_result.get("selected_columns", {}))
        in_paths = set(original_for_compare.get("selected_reference_path", {}).keys()) if isinstance(original_for_compare.get("selected_reference_path"), dict) else set()
        out_paths = set(validated_result.get("selected_reference_path", {}).keys()) if isinstance(validated_result.get("selected_reference_path"), dict) else set()

        st.json({
            "removed_tables": sorted(list(in_tables - out_tables)),
            "added_tables": sorted(list(out_tables - in_tables)),
            "removed_columns": sorted(list(in_columns - out_columns)),
            "added_columns": sorted(list(out_columns - in_columns)),
            "removed_paths": sorted(list(in_paths - out_paths)),
            "added_paths": sorted(list(out_paths - in_paths))
        })

        st.subheader("6) failure_reasons 结构化展示")
        if failure_reasons:
            st.dataframe(pd.DataFrame(failure_reasons), use_container_width=True, hide_index=True)
        else:
            st.info("无 failure_reasons。")

        st.subheader("7) 过滤接口结果（辅助核验）")
        helper_col1, helper_col2 = st.columns(2)
        with helper_col1:
            st.markdown("**filter_valid_tables**")
            st.json(validator.filter_valid_tables(original_for_compare.get("selected_columns", {})))
        with helper_col2:
            st.markdown("**filter_valid_foreign_keys**")
            st.json(validator.filter_valid_foreign_keys(original_for_compare.get("selected_reference_path", {})))

    except json.JSONDecodeError as e:
        st.error(f"JSON 解析失败: {e}")
    except Exception as e:
        st.error(f"执行异常: {e}")
        st.exception(e)


# ==========================================
# 6. 主程序
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

    # 渲染验证器测试沙箱
    render_validator_sandbox(G)


if __name__ == "__main__":
    main()
