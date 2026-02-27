import streamlit as st
import sys
import os
import json
import hashlib
from pathlib import Path
from datetime import datetime
from streamlit_agraph import agraph, Node, Edge, Config

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from configs import paths
from src.utils.graph_loader import GraphLoader
from src.utils.dataloder import DataLoader
import importlib
import src.schema_linking.anchor_selectior
importlib.reload(src.schema_linking.anchor_selectior)
from src.schema_linking.anchor_selectior import run_anchor_selection
from src.utils.sql_parser import SQLParser
from src.llm.clients import LLMClient
from src.utils.experiment_logger import ExperimentLogger

# ==========================================
# 0. Global Config & Styles
# ==========================================

# Graph Data Root
ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")
# Inference Result Root
INFERENCE_RESULT_ROOT = os.path.join(paths.DATA_ROOT, "inference_results", "schema_linking")

# Styles
STYLE = {
    "Table": {
        "color": "#1976D2",
        "font_size": 14,
        "size": 50,
        "font_color": "white"
    },
    "Column": {
        "color": "#81C784",
        "font_size": 10,
        "size": 24,
        "font_color": "black"
    },
    "FOREIGN_KEY": {
        "color": "#E57373",
        "width": 3,
        "dashes": True
    },
    "HAS_COLUMN": {
        "color": "#BDBDBD",
        "width": 1.5,
        "dashes": False
    },
    # LLM Selected Anchor (Keep Orange)
    "Anchor": {
        "color": "#FF9800",
        "borderWidth": 4,
        "font_color": "black",
        "shadow": {"enabled": True, "color": "rgba(255, 152, 0, 0.6)", "size": 10}
    },
    # Ground Truth Node (Tech Light Purple)
    "GroundTruth": {
        "color": "#D0BCFF",  # 科技感淡紫色 (Neon Pale Purple)
        "borderWidth": 3,
        "font_color": "white",  # 如果觉得背景太淡导致白色字体看不清，可以改为 "#333333" 或 "#1A1A24"
        "shadow": {
            "enabled": True,
            "color": "rgba(208, 188, 255, 0.6)",  # 与背景色匹配的发光阴影
            "size": 12  # 稍微调大了阴影尺寸，增强“发光”的科技感
        }
    },
    # False Positive (Red - Selected but not in GT)
    "FalsePositive": {
        "color": "#D32F2F",  # Red
        "borderWidth": 4,
        "font_color": "white",
        "shadow": {"enabled": True, "color": "rgba(211, 47, 47, 0.6)", "size": 10}
    }
}

# ==========================================
# 1. Helper Functions
# ==========================================

def get_subdirs(path):
    if not os.path.exists(path):
        return []
    return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])

def smart_truncate(content, length=8):
    s = str(content)
    if len(s) <= length:
        return s
    return s[:length] + ".."

@st.cache_data
def load_qa_data(dataset_name, db_id):
    try:
        loader_name = dataset_name
        if dataset_name == "spider":
            loader_name = "spider"
        elif dataset_name == "bird":
            loader_name = "bird"
            
        loader = DataLoader(loader_name)
        data = loader.filter_data(db_id=db_id, fields=["question", "sql_query", "evidence"])
        return data
    except Exception as e:
        st.error(f"加载数据失败: {e}")
        return []

@st.cache_data
def load_graph_from_pkl(pkl_path):
    G = GraphLoader.load_graph(pkl_path)
    return G

def get_result_file_path(dataset_name, db_id):
    """Get the path for storing results for a specific DB"""
    folder = os.path.join(INFERENCE_RESULT_ROOT, dataset_name)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{db_id}.json")

def load_existing_results(dataset_name, db_id):
    """Load all results for this DB"""
    path = get_result_file_path(dataset_name, db_id)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"读取历史结果失败: {e}")
    return {}

def save_result(dataset_name, db_id, question_index, question, model_key, result_data):
    """Save a single result"""
    path = get_result_file_path(dataset_name, db_id)
    all_results = load_existing_results(dataset_name, db_id)
    
    # 结构: db_id -> question_index -> {question, schema_linking_results}
    if db_id not in all_results:
        all_results[db_id] = {}
        
    q_idx_str = str(question_index)
    if q_idx_str not in all_results[db_id]:
        all_results[db_id][q_idx_str] = {
            "question": question,
            "schema_linking_results": {}
        }
        
    # 保存本次推理结果，使用 model_key 作为标识
    all_results[db_id][q_idx_str]["schema_linking_results"][model_key] = {
        "result": result_data,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

def extract_gt_entities(dataset_name, db_id, sql):
    """Extract Ground Truth tables and columns from SQL"""
    try:
        parser = SQLParser(dataset_name, db_id)
        # extract_entities returns {table: [cols]}
        entities = parser.extract_entities(sql)
        
        flat_gt_tables = []
        flat_gt_columns = []
        
        for table, cols in entities.items():
            flat_gt_tables.append(table)
            for col in cols:
                # Store full column name "table.col"
                flat_gt_columns.append(f"{table}.{col}")
                
        return entities, flat_gt_tables, flat_gt_columns
    except Exception as e:
        return None, [], []

def convert_nx_to_agraph(G, show_columns=True, selected_nodes=None, gt_nodes=None):
    """
    Convert NetworkX graph to Agraph nodes/edges with styling.
    
    Args:
        selected_nodes: List of nodes selected by LLM (Predictions)
        gt_nodes: List of Ground Truth nodes
    """
    if selected_nodes is None: selected_nodes = []
    if gt_nodes is None: gt_nodes = []
    
    nodes = []
    edges = []
    edge_map = {}
    
    pred_set = set(selected_nodes)
    gt_set = set(gt_nodes)

    for node_id, attrs in G.nodes(data=True):
        node_type = attrs.get("type", "Unknown")
        if not show_columns and node_type == "Column":
            continue

        conf = STYLE.get(node_type, {}).copy()
        
        # --- Styling Logic ---
        # 1. Base style already applied from conf
        
        is_pred = node_id in pred_set
        is_gt = node_id in gt_set
        
        # Result Comparison View (Default)
        if is_pred and is_gt:
             # True Positive -> Keep as Prediction Style (Orange) per user request
             # "LLM选择的节点继续采用原来的黄色样式（不变）"
             conf.update(STYLE["Anchor"])
             # Maybe add a green border to indicate correctness?
             conf["borderWidth"] = 5
             conf["borderColor"] = "#4CAF50" # Green border
             
        elif is_pred and not is_gt:
            # False Positive -> Red
            conf.update(STYLE["FalsePositive"])
            
        elif not is_pred and is_gt:
            # False Negative (Missed GT) -> Purple
            conf.update(STYLE["GroundTruth"])

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
            # Tooltip info
            title=f"Name: {real_name}\nType: {node_type}\nIn Prediction: {is_pred}\nIn Ground Truth: {is_gt}",
            borderWidth=conf.get("borderWidth", 1),
            borderWidthSelected=3,
            shadow=conf.get("shadow", {"enabled": True, "color": "rgba(0,0,0,0.3)", "size": 5, "x": 2, "y": 2})
        ))

    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get("type")
        if not show_columns:
            if edge_type == "HAS_COLUMN": continue
            if G.nodes[u].get("type") == "Column" or G.nodes[v].get("type") == "Column": continue

        conf = STYLE.get(edge_type, {})
        edge_id = f"{u}___{v}___{edge_type}"
        edge_map[edge_id] = attrs
        edges.append(Edge(
            id=edge_id,
            source=u,
            target=v,
            color=conf.get("color"),
            width=conf.get("width"),
            dashes=conf.get("dashes", False),
            arrows={"to": {"enabled": True, "scaleFactor": 0.8}}
        ))

    return nodes, edges, edge_map

# ==========================================
# 2. Sidebar Logic
# ==========================================
def render_sidebar():
    st.sidebar.header("🗄️ 设置与选择")
    
    # 1. Dataset
    available_datasets = get_subdirs(ROOT_DIR)
    if not available_datasets:
        st.sidebar.error(f"No datasets found in {ROOT_DIR}")
        return None, None, None, None, None, None, None, None

    # Load previous selection from session state
    prev_dataset = st.session_state.get('prev_dataset', available_datasets[0])
    try:
        dataset_index = available_datasets.index(prev_dataset)
    except ValueError:
        dataset_index = 0

    selected_dataset = st.sidebar.selectbox("1. 选择数据集", available_datasets, index=dataset_index, key="dataset_selector")
    st.session_state['prev_dataset'] = selected_dataset
    
    # 2. Database
    dataset_path = os.path.join(ROOT_DIR, selected_dataset)
    available_dbs = get_subdirs(dataset_path)
    if not available_dbs:
        return selected_dataset, None, None, None, None, None, None, None
        
    prev_db = st.session_state.get('prev_db', available_dbs[0] if available_dbs else None)
    try:
        db_index = available_dbs.index(prev_db)
    except ValueError:
        db_index = 0
        
    selected_db = st.sidebar.selectbox("2. 选择数据库", available_dbs, index=db_index, key="db_selector")
    st.session_state['prev_db'] = selected_db
    
    # Find PKL
    pkl_file = None
    db_path = os.path.join(dataset_path, selected_db)
    if os.path.exists(db_path):
        for file in os.listdir(db_path):
            if file.endswith(".pkl"):
                pkl_file = os.path.join(db_path, file)
                break
    
    # 3. Model Selection
    st.sidebar.markdown("---")
    st.sidebar.subheader("🤖 模型配置")
    
    providers = ["deepseek", "openai", "gemini", "ollama"]
    prev_provider = st.session_state.get('prev_provider', "deepseek")
    try:
        provider_index = providers.index(prev_provider)
    except ValueError:
        provider_index = 0
        
    model_provider = st.sidebar.selectbox(
        "选择供应商 (Provider)",
        providers,
        index=provider_index,
        key="provider_selector"
    )
    st.session_state['prev_provider'] = model_provider
    
    # Dynamic model options
    model_options = []
    if model_provider == "deepseek":
        model_options = ["deepseek-chat", "deepseek-reasoner"]
    elif model_provider == "openai":
        model_options = ["gpt-4o", "gpt-3.5-turbo"]
    elif model_provider == "gemini":
        model_options = ["gemini-2.0-flash", "gemini-pro"]
    elif model_provider == "ollama":
        try:
            # 动态获取 Ollama 模型列表
            temp_client = LLMClient(provider="ollama")
            ollama_models = temp_client.list_models()
            if ollama_models:
                model_options = ollama_models
            else:
                st.sidebar.warning("Ollama 连接成功但未返回模型列表。")
                model_options = ["llama3", "mistral"]
        except Exception as e:
            st.sidebar.error(f"Ollama 连接失败: {e}")
            model_options = ["llama3", "mistral"]
        
    prev_model = st.session_state.get('prev_model', model_options[0] if model_options else None)
    try:
        model_index = model_options.index(prev_model)
    except ValueError:
        model_index = 0
        
    selected_model = st.sidebar.selectbox("选择模型 (Model)", model_options, index=model_index, key="model_selector")
    st.session_state['prev_model'] = selected_model
    
    # Schema Detail Level
    detail_levels = ["full", "brief", "minimal"]
    prev_detail = st.session_state.get('prev_detail', "full")
    try:
        detail_index = detail_levels.index(prev_detail)
    except ValueError:
        detail_index = 0
        
    schema_detail = st.sidebar.selectbox(
        "Schema 详细程度 (Input Prompt)",
        detail_levels,
        index=detail_index,
        help="控制输入给 LLM 的数据库 Schema 描述的详细程度",
        key="detail_selector"
    )
    st.session_state['prev_detail'] = schema_detail

    # 4. QA Selection
    st.sidebar.markdown("---")
    qa_list = load_qa_data(selected_dataset, selected_db)
    selected_qa = None
    selected_index = 0
    
    if qa_list:
        qa_options = {i: f"{i}. {q['question'][:50]}..." for i, q in enumerate(qa_list)}
        
        # QA selection logic - try to keep index if within range
        prev_qa_index = st.session_state.get('prev_qa_index', 0)
        if prev_qa_index >= len(qa_list):
            prev_qa_index = 0
            
        selected_index = st.sidebar.selectbox(
            "3. 选择测试问题", 
            options=qa_options.keys(), 
            format_func=lambda x: qa_options[x],
            index=prev_qa_index,
            key="qa_selector"
        )
        st.session_state['prev_qa_index'] = selected_index
        selected_qa = qa_list[selected_index]
    else:
        st.sidebar.warning("未找到该数据库的测试问题")

    st.sidebar.markdown("---")
    
    prev_show_cols = st.session_state.get('prev_show_cols', True)
    show_columns = st.sidebar.checkbox("显示列节点", value=prev_show_cols, key="show_cols_selector") # Default to True to show GT columns
    st.session_state['prev_show_cols'] = show_columns
    
    return selected_dataset, selected_db, selected_qa, selected_index, pkl_file, show_columns, model_provider, selected_model, schema_detail

# ==========================================
# 3. Main Logic
# ==========================================
def main():
    st.title("⚓ Schema Linking & Anchor Selection")
    st.set_page_config(layout="wide")
    (selected_dataset, selected_db, selected_qa, selected_index, pkl_file, 
     show_columns, model_provider, selected_model, schema_detail) = render_sidebar()
    
    if not pkl_file or not selected_qa:
        st.info("👈 请在左侧选择完整的数据集、数据库和问题以开始")
        return

    # --- GT Analysis ---
    st.subheader("📋 任务详情与真值解析")
    
    col_q, col_sql = st.columns([1, 1])
    with col_q:
        st.markdown("**User Question:**")
        st.info(selected_qa['question'])
        if selected_qa.get('evidence'):
            st.markdown("**Evidence (BIRD):**")
            st.warning(selected_qa['evidence'])
            
    with col_sql:
        st.markdown("**Ground Truth SQL:**")
        st.code(selected_qa['sql_query'], language="sql")

    # Extract GT Entities
    gt_entities_dict, gt_flat_tables, gt_flat_columns = extract_gt_entities(selected_dataset, selected_db, selected_qa['sql_query'])
    
    # Combine for visualization highlighting (IDs in graph are TableName or TableName.ColumnName)
    gt_all_nodes = gt_flat_tables + gt_flat_columns
    
    with st.expander("🔍 查看 SQL 解析结果 (Ground Truth Schema Items)", expanded=False):
        if gt_entities_dict:
            st.write("从 SQL 中解析出的表与列：")
            st.json(gt_entities_dict)
        else:
            st.warning("SQL 解析失败或未找到实体，可能 SQL 语法复杂或 Schema 不匹配。")

    st.markdown("---")

    # --- Anchor Selection Action ---
    st.subheader("🤖 模型推理与对比")
    
    col_action, col_hist = st.columns([1, 2])
    
    # Unique Key for this run
    # q_hash = hashlib.md5(selected_qa['question'].encode('utf-8')).hexdigest() # No longer used for lookup
    model_key = f"{model_provider}@{selected_model}@{schema_detail}"
    
    # Check history
    # 使用 ExperimentLogger 加载历史
    logger = ExperimentLogger(selected_dataset, "schema_linking")
    history = logger.load_results(selected_db)
    
    existing_result = None
    q_idx_str = str(selected_index)
    
    if selected_db in history and q_idx_str in history[selected_db]:
        question_entry = history[selected_db][q_idx_str]
        if "schema_linking_results" in question_entry and model_key in question_entry["schema_linking_results"]:
            existing_result = question_entry["schema_linking_results"][model_key]

    # Clear current_result if it belongs to a different question/db
    if 'current_result_meta' in st.session_state:
        meta = st.session_state.current_result_meta
        if meta.get('db') != selected_db or meta.get('q_idx') != selected_index or meta.get('model') != model_key:
             st.session_state.current_result = None
             
    # Run Button
    with col_action:
        if existing_result:
            st.success(f"已发现历史记录 ({existing_result['timestamp'][:16]})")
        
        # Two buttons logic
        # 1. SL1 Only
        if st.button("🚀 仅运行锚点选择 (SL1)", type="secondary", use_container_width=True):
             with st.spinner(f"正在调用 {model_key} 进行 SL1 推理..."):
                result = run_anchor_selection(
                    dataset_name=selected_dataset,
                    db_id=selected_db,
                    question=selected_qa['question'],
                    provider=model_provider,
                    model=selected_model,
                    schema_detail_level=schema_detail,
                    run_sl2=False # Disable SL2
                )
                if "error" not in result:
                    # 使用 ExperimentLogger 保存结果
                    logger.save_result(
                        db_id=selected_db,
                        question_index=selected_index,
                        question=selected_qa['question'],
                        model_key=model_key,
                        result_data=result,
                        ground_truth_sql=selected_qa['sql_query'],
                        evidence=selected_qa.get('evidence')
                    )
                    st.session_state.current_result = result
                    st.session_state.current_result_meta = {
                        'db': selected_db,
                        'q_idx': selected_index,
                        'model': model_key
                    }
                    st.rerun() # Refresh to show saved result
                else:
                    st.error(f"推理出错: {result['error']}")

        # 2. Full Pipeline
        if st.button("🚀 运行全流程 (SL1 + SL2)", type="primary", use_container_width=True):
            with st.spinner(f"正在调用 {model_key} 进行全流程推理..."):
                result = run_anchor_selection(
                    dataset_name=selected_dataset,
                    db_id=selected_db,
                    question=selected_qa['question'],
                    provider=model_provider,
                    model=selected_model,
                    schema_detail_level=schema_detail,
                    run_sl2=True # Enable SL2
                )
                if "error" not in result:
                    # 使用 ExperimentLogger 保存结果
                    logger.save_result(
                        db_id=selected_db,
                        question_index=selected_index,
                        question=selected_qa['question'],
                        model_key=model_key,
                        result_data=result,
                        ground_truth_sql=selected_qa['sql_query'],
                        evidence=selected_qa.get('evidence')
                    )
                    st.session_state.current_result = result
                    st.session_state.current_result_meta = {
                        'db': selected_db,
                        'q_idx': selected_index,
                        'model': model_key
                    }
                    st.rerun() # Refresh to show saved result
                else:
                    st.error(f"推理出错: {result['error']}")

    # Display Result
    display_result = None
    
    # Priority 1: Just run result (in session state)
    if st.session_state.get('current_result'):
        display_result = st.session_state.current_result
    
    # Priority 2: Historical result (if no fresh run)
    if not display_result and existing_result:
        display_result = existing_result["result"]

    selected_anchors = []
    prompts_used = {}
    
    if display_result:
        # Check if display_result has the new structure with step2_route_result
        if "step2_route_result" in display_result:
             # Use the final subgraph nodes from the routing step
             selected_anchors = display_result.get("final_subgraph_nodes", [])
             
             # Extract prompts from step1_llm_result (if available)
             step1 = display_result.get("step1_llm_result", {})
             prompts_used = step1.get("_prompts", {})
             
             # Show Route Status
             st.info(f"🚦 Routing Status: **{display_result.get('status')}**")
             
             with st.expander("查看路由决策详情 (Routing Details)"):
                 route_res = display_result.get("step2_route_result", {})
                 
                 # Compare initial LLM anchors with final subgraph nodes
                 initial = set(display_result.get("step1_llm_result", {}).get("selected_entity", []))
                 # Only keep table names from initial (remove columns)
                 initial_tables = set([x.split('.')[0] for x in initial])
                 
                 final = set(selected_anchors)
                 
                 added = final - initial_tables
                 removed = initial_tables - final # Should be empty ideally unless filtered
                 
                 c1, c2 = st.columns(2)
                 with c1:
                     st.write("🔹 **Initial Anchors (LLM):**")
                     st.write(list(initial_tables))
                 with c2:
                     st.write("🔸 **Added by Graph Algorithm:**")
                     if added:
                         st.success(list(added))
                     else:
                         st.caption("No extra nodes added.")
                         
                 st.json(route_res)
                 
        else:
            # Fallback for old result format (just LLM result)
            selected_anchors = display_result.get("selected_entity", [])
            prompts_used = display_result.get("_prompts", {})
            
        # --- Show Prompts ---
        if prompts_used:
            with st.expander("📝 查看输入 Prompt (System & User)", expanded=False):
                st.markdown("#### System Prompt")
                st.text(prompts_used.get("system", "N/A"))
                st.markdown("#### User Prompt")
                st.text(prompts_used.get("user", "N/A"))
            
        # --- Show Raw Output ---
        with st.expander("👀 查看 LLM 详细输出信息 (Raw Output)", expanded=False):
            st.json(display_result)
        
        # --- Comparison Logic ---
        # Separate Table and Column Stats
        
        # 1. Table Stats (Main Focus)
        pred_tables = [x for x in selected_anchors if "." not in x]
        gt_tables = gt_flat_tables
        
        gt_t_set = set(gt_tables)
        pred_t_set = set(pred_tables)
        
        tp_t = gt_t_set.intersection(pred_t_set)
        fp_t = pred_t_set - gt_t_set
        fn_t = gt_t_set - pred_t_set
        
        st.markdown("#### 📊 表节点命中统计 (Table Hit Stats)")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("✅ 正确表 (TP)", len(tp_t))
            if tp_t: st.success(list(tp_t))
        with c2:
            st.metric("❌ 多选表 (FP)", len(fp_t))
            if fp_t: st.error(list(fp_t))
        with c3:
            st.metric("⚠️ 漏选表 (FN)", len(fn_t))
            if fn_t: st.warning(list(fn_t))
            
        # 2. Column Stats (Secondary)
        # Note: Current Anchor Selector might ONLY return tables, not columns.
        # If it returns columns (e.g. "table.col"), we count them.
        pred_cols = [x for x in selected_anchors if "." in x]
        gt_cols = gt_flat_columns
        
        if gt_cols: # Only show if there are GT columns to match
            gt_c_set = set(gt_cols)
            pred_c_set = set(pred_cols)
            
            tp_c = gt_c_set.intersection(pred_c_set)
            fp_c = pred_c_set - gt_c_set
            fn_c = gt_c_set - pred_c_set
            
            with st.expander("查看列节点命中详情 (Column Hit Stats - 目前仅作参考)"):
                cc1, cc2, cc3 = st.columns(3)
                with cc1:
                    st.metric("✅ 正确列", len(tp_c))
                    if tp_c: st.write(list(tp_c))
                with cc2:
                    st.metric("❌ 多选列", len(fp_c))
                    if fp_c: st.write(list(fp_c))
                with cc3:
                    st.metric("⚠️ 漏选列", len(fn_c))
                    if fn_c: st.write(list(fn_c))

    # --- Graph Visualization ---
    st.markdown("---")
    st.subheader("🕸️ 数据库图结构可视化 (Schema Graph Visualization)")
    
    # Check if we have SL2 iterations to show
    sl2_data = display_result.get("step3_expansion_result") if display_result else None
    
    if sl2_data and "iterations" in sl2_data:
        st.info("检测到 SL2 迭代扩展数据，启用分步可视化模式。")
        
        iterations = sl2_data["iterations"]
        # Create tabs for each iteration + Final Result
        tab_labels = [f"Iteration {i['iteration']}" for i in iterations] + ["🏁 最终结果 (Final)"]
        tabs = st.tabs(tab_labels)
        
        G = load_graph_from_pkl(pkl_file)
        
        # --- Iteration Tabs ---
        for idx, it_data in enumerate(iterations):
            with tabs[idx]:
                st.markdown(f"#### 🔄 Iteration {it_data['iteration']}")
                
                # Show Prompts & Reasoning
                with st.expander("📝 Iteration Prompt & Response", expanded=False):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("**User Prompt:**")
                        st.text(it_data.get("prompts", {}).get("user", ""))
                    with c2:
                        st.markdown("**LLM Response:**")
                        st.json(it_data.get("llm_response", {}))
                        
                # Graph State
                core = it_data.get("core_tables", [])
                frontier = it_data.get("frontier_tables", [])
                selected_frontier = it_data.get("llm_response", {}).get("selected_tables_from_frontier", [])
                selected_cols_dict = it_data.get("llm_response", {}).get("selected_columns_from_core", {})
                
                # Construct "Current Prediction" nodes for this iteration
                # Prediction = Core Tables + Selected Frontier Tables + Selected Columns
                current_pred_nodes = set(core)
                current_pred_nodes.update(selected_frontier)
                
                # Add columns
                for table, cols in selected_cols_dict.items():
                    # We need to match column names to node IDs. 
                    # Usually "Table.Column". We can try to guess or just add them and let logic handle it.
                    # Or better, iterate cols and try "Table.Col" format.
                    if isinstance(cols, list):
                        for c in cols:
                            current_pred_nodes.add(f"{table}.{c}")
                    elif isinstance(cols, str):
                         current_pred_nodes.add(f"{table}.{cols}")

                # Legend for Iteration (Standard)
                st.markdown("""
                <div style="display: flex; gap: 20px; margin-bottom: 10px;">
                    <div><span style="color:#FF9800; font-size:20px;">●</span> <b>模型预测 (Prediction)</b></div>
                    <div><span style="color:#9C27B0; font-size:20px;">●</span> <b>真值漏选 (Missed GT)</b></div>
                    <div><span style="color:#D32F2F; font-size:20px;">●</span> <b>错误多选 (False Positive)</b></div>
                </div>
                """, unsafe_allow_html=True)
                
                if G:
                    nodes, edges, _ = convert_nx_to_agraph(
                        G, 
                        show_columns=True, # Show columns to see column hits
                        selected_nodes=list(current_pred_nodes),
                        gt_nodes=gt_all_nodes
                    )
                    config = Config(width="100%", height=600, directed=True, physics=True, hierarchical=False)
                    agraph(nodes=nodes, edges=edges, config=config)

        # --- Final Result Tab ---
        with tabs[-1]:
            st.markdown("#### 🏁 Final Subgraph vs Ground Truth")
            # Legend
            st.markdown("""
            <div style="display: flex; gap: 20px; margin-bottom: 10px;">
                <div><span style="color:#FF9800; font-size:20px;">●</span> <b>模型预测 (Prediction)</b></div>
                <div><span style="color:#9C27B0; font-size:20px;">●</span> <b>真值漏选 (Missed GT)</b></div>
                <div><span style="color:#D32F2F; font-size:20px;">●</span> <b>错误多选 (False Positive)</b></div>
            </div>
            """, unsafe_allow_html=True)
            
            if G:
                nodes, edges, _ = convert_nx_to_agraph(
                    G, 
                    show_columns, 
                    selected_nodes=selected_anchors,
                    gt_nodes=gt_all_nodes
                )
                config = Config(
                    width="100%", height=800, directed=True, physics=True, hierarchical=False,
                    physicsOptions={"barnesHut": {"gravitationalConstant": -5000, "springLength": 220, "springConstant": 0.05}}
                )
                agraph(nodes=nodes, edges=edges, config=config)

    else:
        # Standard View (No SL2 or SL2 failed)
        # Legend
        st.markdown("""
        <div style="display: flex; gap: 20px; margin-bottom: 10px;">
            <div><span style="color:#FF9800; font-size:20px;">●</span> <b>模型预测 (Prediction)</b></div>
            <div><span style="color:#9C27B0; font-size:20px;">●</span> <b>真值漏选 (Missed GT)</b></div>
            <div><span style="color:#D32F2F; font-size:20px;">●</span> <b>错误多选 (False Positive)</b></div>
            <div><span style="color:#1976D2; font-size:20px;">●</span> 普通表节点</div>
        </div>
        """, unsafe_allow_html=True)
        
        G = load_graph_from_pkl(pkl_file)
        
        if G:
            nodes, edges, edge_map = convert_nx_to_agraph(
                G, 
                show_columns, 
                selected_nodes=selected_anchors,
                gt_nodes=gt_all_nodes
            )
            
            config = Config(
                width="100%",
                height=800,
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
            
            agraph(nodes=nodes, edges=edges, config=config)

if __name__ == "__main__":
    main()
