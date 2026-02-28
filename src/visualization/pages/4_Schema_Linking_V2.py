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
from src.utils.sql_parser import SQLParser
from src.llm.clients import LLMClient
from src.utils.experiment_logger import ExperimentLogger
from src.schema_linking_v2.pipeline import SchemaLinkingPipelineV2

# ==========================================
# 0. Global Config & Styles
# ==========================================

# Graph Data Root
ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")
# Inference Result Root
INFERENCE_RESULT_ROOT = os.path.join(paths.DATA_ROOT, "inference_results", "schema_linking_v2")

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
    "Anchor": {
        "color": "#FF9800",
        "borderWidth": 4,
        "font_color": "black",
        "shadow": {"enabled": True, "color": "rgba(255, 152, 0, 0.6)", "size": 10}
    },
    "GroundTruth": {
        "color": "#D0BCFF",
        "borderWidth": 3,
        "font_color": "white",
        "shadow": {
            "enabled": True,
            "color": "rgba(208, 188, 255, 0.6)",
            "size": 12
        }
    },
    "FalsePositive": {
        "color": "#D32F2F",
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

def extract_gt_entities(dataset_name, db_id, sql):
    """Extract Ground Truth tables and columns from SQL"""
    try:
        parser = SQLParser(dataset_name, db_id)
        entities = parser.extract_entities(sql)
        
        flat_gt_tables = []
        flat_gt_columns = []
        
        for table, cols in entities.items():
            flat_gt_tables.append(table)
            for col in cols:
                flat_gt_columns.append(f"{table}.{col}")
                
        return entities, flat_gt_tables, flat_gt_columns
    except Exception as e:
        return None, [], []

def convert_nx_to_agraph(G, show_columns=True, selected_nodes=None, gt_nodes=None):
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
        
        is_pred = node_id in pred_set
        is_gt = node_id in gt_set
        
        if is_pred and is_gt:
             conf.update(STYLE["Anchor"])
             conf["borderWidth"] = 5
             conf["borderColor"] = "#4CAF50"
             
        elif is_pred and not is_gt:
            conf.update(STYLE["FalsePositive"])
            
        elif not is_pred and is_gt:
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

def display_prompt_response(title, prompts, response, expanded=False):
    """
    Helper to display prompts and response side-by-side.
    prompts: {"system": ..., "user": ...}
    response: JSON object or string
    """
    with st.expander(title, expanded=expanded):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 📤 Input (Prompts)")
            if isinstance(prompts, dict):
                st.markdown("**System Prompt:**")
                st.code(prompts.get("system", ""), language="text")
                st.markdown("**User Prompt:**")
                st.code(prompts.get("user", ""), language="text")
            else:
                st.code(str(prompts))
        with c2:
            st.markdown("#### 📥 Output (Response)")
            st.json(response)

# ==========================================
# 2. Sidebar Logic
# ==========================================
def render_sidebar():
    st.sidebar.header("🗄️ 设置与选择 (V2)")
    
    # 1. Dataset
    available_datasets = get_subdirs(ROOT_DIR)
    if not available_datasets:
        st.sidebar.error(f"No datasets found in {ROOT_DIR}")
        return None, None, None, None, None, None, None, None

    prev_dataset = st.session_state.get('prev_dataset_v2', available_datasets[0])
    try:
        dataset_index = available_datasets.index(prev_dataset)
    except ValueError:
        dataset_index = 0

    selected_dataset = st.sidebar.selectbox("1. 选择数据集", available_datasets, index=dataset_index, key="dataset_selector_v2")
    st.session_state['prev_dataset_v2'] = selected_dataset
    
    # 2. Database
    dataset_path = os.path.join(ROOT_DIR, selected_dataset)
    available_dbs = get_subdirs(dataset_path)
    if not available_dbs:
        return selected_dataset, None, None, None, None, None, None, None
        
    prev_db = st.session_state.get('prev_db_v2', available_dbs[0] if available_dbs else None)
    try:
        db_index = available_dbs.index(prev_db)
    except ValueError:
        db_index = 0
        
    selected_db = st.sidebar.selectbox("2. 选择数据库", available_dbs, index=db_index, key="db_selector_v2")
    st.session_state['prev_db_v2'] = selected_db
    
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
    prev_provider = st.session_state.get('prev_provider_v2', "deepseek")
    try:
        provider_index = providers.index(prev_provider)
    except ValueError:
        provider_index = 0
        
    model_provider = st.sidebar.selectbox(
        "选择供应商 (Provider)",
        providers,
        index=provider_index,
        key="provider_selector_v2"
    )
    st.session_state['prev_provider_v2'] = model_provider
    
    model_options = []
    if model_provider == "deepseek":
        model_options = ["deepseek-chat", "deepseek-reasoner"]
    elif model_provider == "openai":
        model_options = ["gpt-4o", "gpt-3.5-turbo"]
    elif model_provider == "gemini":
        model_options = ["gemini-2.0-flash", "gemini-pro"]
    elif model_provider == "ollama":
        try:
            temp_client = LLMClient(provider="ollama")
            ollama_models = temp_client.list_models()
            if ollama_models:
                model_options = ollama_models
            else:
                model_options = ["llama3", "mistral"]
        except Exception as e:
            model_options = ["llama3", "mistral"]
        
    prev_model = st.session_state.get('prev_model_v2', model_options[0] if model_options else None)
    try:
        model_index = model_options.index(prev_model)
    except ValueError:
        model_index = 0
        
    selected_model = st.sidebar.selectbox("选择模型 (Model)", model_options, index=model_index, key="model_selector_v2")
    st.session_state['prev_model_v2'] = selected_model
    
    # 4. QA Selection
    st.sidebar.markdown("---")
    qa_list = load_qa_data(selected_dataset, selected_db)
    selected_qa = None
    selected_index = 0
    
    if qa_list:
        qa_options = {i: f"{i}. {q['question'][:50]}..." for i, q in enumerate(qa_list)}
        prev_qa_index = st.session_state.get('prev_qa_index_v2', 0)
        if prev_qa_index >= len(qa_list):
            prev_qa_index = 0
            
        selected_index = st.sidebar.selectbox(
            "3. 选择测试问题", 
            options=qa_options.keys(), 
            format_func=lambda x: qa_options[x],
            index=prev_qa_index,
            key="qa_selector_v2"
        )
        st.session_state['prev_qa_index_v2'] = selected_index
        selected_qa = qa_list[selected_index]
    else:
        st.sidebar.warning("未找到该数据库的测试问题")

    st.sidebar.markdown("---")
    
    prev_show_cols = st.session_state.get('prev_show_cols_v2', True)
    show_columns = st.sidebar.checkbox("显示列节点", value=prev_show_cols, key="show_cols_selector_v2")
    st.session_state['prev_show_cols_v2'] = show_columns
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🎛️ Schema 详略控制")
    
    detail_levels = ["full", "brief", "minimal"]
    
    sl1_detail = st.sidebar.selectbox("SL1 初始选表", detail_levels, index=1, key="sl1_detail") # Default brief
    sl2_detail = st.sidebar.selectbox("SL2 子图扩展", detail_levels, index=1, key="sl2_detail") # Default brief
    sl3_detail = st.sidebar.selectbox("SL3 候选优选", detail_levels, index=0, key="sl3_detail") # Default full
    
    return selected_dataset, selected_db, selected_qa, selected_index, pkl_file, show_columns, model_provider, selected_model, sl1_detail, sl2_detail, sl3_detail

# ==========================================
# 3. Main Logic
# ==========================================
def main():
    st.title("⚓ Schema Linking V2 (Pipeline)")
    st.set_page_config(layout="wide")
    (selected_dataset, selected_db, selected_qa, selected_index, pkl_file, 
     show_columns, model_provider, selected_model, sl1_detail, sl2_detail, sl3_detail) = render_sidebar()
    
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

    gt_entities_dict, gt_flat_tables, gt_flat_columns = extract_gt_entities(selected_dataset, selected_db, selected_qa['sql_query'])
    gt_all_nodes = gt_flat_tables + gt_flat_columns
    
    with st.expander("🔍 查看 SQL 解析结果 (Ground Truth Schema Items)", expanded=False):
        if gt_entities_dict:
            st.write("从 SQL 中解析出的表与列：")
            st.json(gt_entities_dict)

    st.markdown("---")

    # --- Initial Graph Structure ---
    st.subheader("🕸️ 初始数据库图结构 (Ground Truth View)")
    st.info("图中紫色节点代表 Ground Truth 表/列。")
    
    model_key = f"{model_provider}@{selected_model}@v2"
    logger = ExperimentLogger(selected_dataset, "schema_linking_v2") 
    history = logger.load_results(selected_db)
    
    existing_result = None
    q_idx_str = str(selected_index)
    
    if selected_db in history and q_idx_str in history[selected_db]:
        question_entry = history[selected_db][q_idx_str]
        if "schema_linking_results" in question_entry and model_key in question_entry["schema_linking_results"]:
            existing_result = question_entry["schema_linking_results"][model_key]

    if 'current_result_meta_v2' in st.session_state:
        meta = st.session_state.current_result_meta_v2
        if meta.get('db') != selected_db or meta.get('q_idx') != selected_index or meta.get('model') != model_key:
             st.session_state.current_result_v2 = None

    if 'streaming_sl1_v2' not in st.session_state: st.session_state.streaming_sl1_v2 = None
    if 'streaming_sl1_prompts_v2' not in st.session_state: st.session_state.streaming_sl1_prompts_v2 = {}
    if 'streaming_sl2_v2' not in st.session_state: st.session_state.streaming_sl2_v2 = {}
    if 'streaming_sl3_v2' not in st.session_state: st.session_state.streaming_sl3_v2 = None
    if 'streaming_sl3_prompts_v2' not in st.session_state: st.session_state.streaming_sl3_prompts_v2 = {}
    if 'streaming_status_v2' not in st.session_state: st.session_state.streaming_status_v2 = "idle"

    # --- Graph Visualization (Updated with Prediction) ---
    
    # Determine nodes to highlight
    pred_nodes = set()
    
    # Check if we have a current result (either from run or history)
    if st.session_state.get('current_result_v2'):
         final_result = st.session_state.current_result_v2.get("final_result", {})
         selected_columns = final_result.get("selected_columns", {})
         for table, cols in selected_columns.items():
            pred_nodes.add(table)
            for col in cols:
                if col in ["*", ""]: continue
                if "." in col:
                    pred_nodes.add(col)
                else:
                    pred_nodes.add(f"{table}.{col}")
    
    st.subheader("🕸️ 数据库图结构 (Graph View)")
    
    st.markdown("""
    <div style="display: flex; gap: 20px; margin-bottom: 10px;">
        <div><span style="color:#FF9800; font-size:20px;">●</span> <b>模型预测 (Prediction)</b></div>
        <div><span style="color:#9C27B0; font-size:20px;">●</span> <b>真值漏选 (Missed GT)</b></div>
        <div><span style="color:#D32F2F; font-size:20px;">●</span> <b>错误多选 (False Positive)</b></div>
    </div>
    """, unsafe_allow_html=True)

    G = load_graph_from_pkl(pkl_file)
    if G:
        nodes, edges, edge_map = convert_nx_to_agraph(
            G, 
            show_columns, 
            selected_nodes=list(pred_nodes),
            gt_nodes=gt_all_nodes
        )
        
        config = Config(
            width="100%",
            height=600,
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

    st.markdown("---")

    # Move Run Button Here
    col_run, col_history = st.columns([2, 1])
    with col_run:
        run_btn = st.button("🚀 运行 Schema Linking V2", type="primary", use_container_width=True)
    with col_history:
        if existing_result:
            if st.button("📥 加载历史记录", use_container_width=True):
                 st.session_state.current_result_v2 = existing_result["result"]
                 st.session_state.current_result_meta_v2 = {
                    'db': selected_db,
                    'q_idx': selected_index,
                    'model': model_key
                 }
                 # Load history into streaming state for detail view
                 st.session_state.streaming_sl1_v2 = None # Clear detailed streaming state when loading static history
                 st.session_state.streaming_sl1_prompts_v2 = {}
                 st.session_state.streaming_sl2_v2 = {}
                 st.session_state.streaming_sl3_v2 = None
                 st.session_state.streaming_sl3_prompts_v2 = {}
                 st.rerun()
        else:
            st.button("暂无历史记录", disabled=True, use_container_width=True)

    if existing_result:
        st.success(f"已发现历史记录 ({existing_result['timestamp'][:16]})")

    if run_btn:
        st.session_state.streaming_status_v2 = "running"
        st.session_state.streaming_sl1_v2 = None
        st.session_state.streaming_sl1_prompts_v2 = {}
        st.session_state.streaming_sl2_v2 = {}
        st.session_state.streaming_sl3_v2 = None
        st.session_state.streaming_sl3_prompts_v2 = {}
        st.session_state.current_result_v2 = None # Clear final result
        
        # Create placeholders
        sl1_container = st.container()
        sl2_container = st.container()
        sl3_container = st.container()
        
        with st.status("正在运行 Pipeline...", expanded=True) as status:
            try:
                pipeline = SchemaLinkingPipelineV2(
                    dataset_name=selected_dataset,
                    db_name=selected_db,
                    question_data=selected_qa
                )
                pipeline.sl1.client = LLMClient(provider=model_provider, model=selected_model)
                pipeline.sl2.client = LLMClient(provider=model_provider, model=selected_model)
                pipeline.sl3.client = LLMClient(provider=model_provider, model=selected_model)
                
                # Set detail levels
                pipeline.sl1_detail = sl1_detail
                pipeline.sl2_detail = sl2_detail
                pipeline.sl3_detail = sl3_detail
                
                final_res = None
                
                # Iterate through stream
                for event in pipeline.run_stream():
                    step = event["step"]
                    
                    if step == "sl1_start":
                        status.write("正在执行 SL1: 初始表选择...")
                    
                    elif step == "sl1_complete":
                        st.session_state.streaming_sl1_v2 = event["result"]
                        st.session_state.streaming_sl1_prompts_v2 = event.get("prompts", {})
                        
                        status.write("✅ SL1 完成")
                        # Render SL1 immediately
                        with sl1_container:
                            res = event["result"]
                            display_prompt_response(
                                f"Step 1: 初始表选择 (Selected: {res.get('selected_entity')})", 
                                event.get("prompts", {}), 
                                res, 
                                expanded=True
                            )
                            
                    elif step == "sl2_start":
                        status.write(f"正在执行 SL2: 并发扩展 {len(event['selected_tables'])} 个表...")
                        
                    elif step == "sl2_table_complete":
                        table = event["table"]
                        table_res = event["result"]
                        # table_res has "history" where prompts are
                        st.session_state.streaming_sl2_v2[table] = table_res
                        status.write(f"✅ SL2 表 {table} 完成")
                        # Render SL2 table update
                        with sl2_container:
                            with st.expander(f"Step 2 (Table: {table})", expanded=False):
                                history = table_res.get("history", [])
                                for i, h in enumerate(history):
                                    st.markdown(f"**Iteration {h['iteration']}**")
                                    display_prompt_response(
                                        f"Iteration {h['iteration']} Details",
                                        h.get("prompts", {}),
                                        h["llm_response"],
                                        expanded=False
                                    )
                                st.markdown("**Final Result for Table**")
                                st.json(table_res.get("final_result", {}))

                    elif step == "sl2_table_error":
                        status.write(f"❌ SL2 表 {event['table']} 失败: {event['error']}")
                        
                    elif step == "sl3_start":
                        status.write("正在执行 SL3: 候选结果优选...")
                        
                    elif step == "sl3_complete":
                        st.session_state.streaming_sl3_v2 = event["result"]
                        st.session_state.streaming_sl3_prompts_v2 = event.get("prompts", {})
                        
                        final_res = event["result"]
                        status.write("✅ SL3 完成")
                        with sl3_container:
                             res = event["result"]["final_result"]
                             display_prompt_response(
                                 "Step 3: 最终候选选择",
                                 event.get("prompts", {}),
                                 res,
                                 expanded=True
                             )
                
                if final_res:
                    st.session_state.current_result_v2 = final_res
                    st.session_state.current_result_meta_v2 = {
                        'db': selected_db,
                        'q_idx': selected_index,
                        'model': model_key
                    }
                    st.session_state.streaming_status_v2 = "done"
                    # Save result
                    logger.save_result(
                        db_id=selected_db,
                        question_index=selected_index,
                        question=selected_qa['question'],
                        model_key=model_key,
                        result_data=final_res,
                        ground_truth_sql=selected_qa['sql_query'],
                        evidence=selected_qa.get('evidence')
                    )
                    status.update(label="Pipeline 执行完毕", state="complete", expanded=False)
                    st.rerun()
                    
            except Exception as e:
                st.error(f"Pipeline Error: {e}")
                status.update(label="Pipeline 执行出错", state="error")

    # Display Static Results (if available and not running)
    if st.session_state.get('current_result_v2') and st.session_state.streaming_status_v2 != "running":
        # If we just finished running, we have the detailed info in streaming_slX_v2 vars
        # If we loaded from history, we only have final result structure.
        # So we need to handle both cases.
        
        result_data = st.session_state.current_result_v2
        
        # 1. SL1
        if st.session_state.streaming_sl1_v2:
            res = st.session_state.streaming_sl1_v2
            prompts = st.session_state.get("streaming_sl1_prompts_v2", {}) or result_data.get("_prompts", {}).get("sl1", {})
            display_prompt_response(f"Step 1: 初始表选择 (Selected: {res.get('selected_entity')})", prompts, res, expanded=False)
        else:
            # Loaded from history
            sl1_res = result_data.get("sl1_result", {})
            prompts = result_data.get("_prompts", {}).get("sl1", {})
            
            display_prompt_response(f"Step 1: 初始表选择 (History)", prompts, sl1_res, expanded=False)

        # 2. SL2
        if st.session_state.streaming_sl2_v2:
            st.markdown("### Step 2: 子图扩展详情")
            tabs = st.tabs(list(st.session_state.streaming_sl2_v2.keys()))
            for i, (table, res) in enumerate(st.session_state.streaming_sl2_v2.items()):
                with tabs[i]:
                    history = res.get("history", [])
                    for h in history:
                         display_prompt_response(
                            f"Iteration {h['iteration']}",
                            h.get("prompts", {}),
                            h["llm_response"],
                            expanded=False
                        )
        else:
            candidate_results = result_data.get("candidate_results", {})
            with st.expander(f"Step 2: 子图扩展候选 (History) - {len(candidate_results)} items", expanded=False):
                # History doesn't have detailed SL2 prompts unless we save full history
                # Currently we only save final candidate results.
                st.json(candidate_results)

        # 3. SL3
        final_result = result_data.get("final_result", {})
        
        prompts = st.session_state.get("streaming_sl3_prompts_v2", {}) or result_data.get("_prompts", {}).get("sl3", {})
             
        display_prompt_response("Step 3: 最终候选选择", prompts, final_result, expanded=True)

        # --- Graph Visualization ---
        st.markdown("### 🏁 最终结果统计 (Final Prediction)")
        
        selected_columns = final_result.get("selected_columns", {})
        
        # Stats
        pred_tables = list(selected_columns.keys())
        gt_tables = gt_flat_tables
        
        gt_t_set = set(gt_tables)
        pred_t_set = set(pred_tables)
        
        tp_t = gt_t_set.intersection(pred_t_set)
        fp_t = pred_t_set - gt_t_set
        fn_t = gt_t_set - pred_t_set
        
        # Calculate Column Stats
        pred_cols = set()
        for t, cols in selected_columns.items():
            for c in cols:
                if c in ["*", ""]: continue
                if "." in c:
                    pred_cols.add(c)
                else:
                    pred_cols.add(f"{t}.{c}")
        
        gt_c_set = set(gt_flat_columns)
        tp_c = gt_c_set.intersection(pred_cols)
        fp_c = pred_cols - gt_c_set
        fn_c = gt_c_set - pred_cols
        
        st.markdown("#### 表级匹配 (Table-level)")
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

        st.markdown("#### 列级匹配 (Column-level)")
        c4, c5, c6 = st.columns(3)
        with c4:
            st.metric("✅ 正确列 (TP)", len(tp_c))
            if tp_c: st.success(list(tp_c))
        with c5:
            st.metric("❌ 多选列 (FP)", len(fp_c))
            if fp_c: st.error(list(fp_c))
        with c6:
            st.metric("⚠️ 漏选列 (FN)", len(fn_c))
            if fn_c: st.warning(list(fn_c))

if __name__ == "__main__":
    main()
