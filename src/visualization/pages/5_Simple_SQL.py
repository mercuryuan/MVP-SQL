import streamlit as st
import sys
import os
import json
from pathlib import Path
import logging
from streamlit_agraph import agraph, Node, Edge, Config

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from configs import paths
from src.utils.dataloder import DataLoader
from src.utils.graph_loader import GraphLoader
from src.utils.sql_parser import SQLParser
from src.llm.clients import LLMClient
from src.simple_sql.pipeline import SimpleSQLPipeline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# 0. Global Config
# ==========================================

# Graph Data Root
ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")

# Styles (Copied from Page 4)
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

def smart_truncate(content, length=8):
    s = str(content)
    if len(s) <= length:
        return s
    return s[:length] + ".."

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

# ==========================================
# 2. Sidebar Logic
# ==========================================
def render_sidebar():
    st.sidebar.header("🗄️ 设置与选择 (Simple SQL)")
    
    # 1. Dataset
    available_datasets = get_subdirs(ROOT_DIR)
    if not available_datasets:
        st.sidebar.error(f"No datasets found in {ROOT_DIR}")
        return None, None, None, None, None, None

    prev_dataset = st.session_state.get('prev_dataset_simple', available_datasets[0])
    try:
        dataset_index = available_datasets.index(prev_dataset)
    except ValueError:
        dataset_index = 0

    selected_dataset = st.sidebar.selectbox("1. 选择数据集", available_datasets, index=dataset_index, key="dataset_selector_simple")
    st.session_state['prev_dataset_simple'] = selected_dataset
    
    # 2. Database
    dataset_path = os.path.join(ROOT_DIR, selected_dataset)
    available_dbs = get_subdirs(dataset_path)
    if not available_dbs:
        return selected_dataset, None, None, None, None, None
        
    prev_db = st.session_state.get('prev_db_simple', available_dbs[0] if available_dbs else None)
    try:
        db_index = available_dbs.index(prev_db)
    except ValueError:
        db_index = 0
        
    selected_db = st.sidebar.selectbox("2. 选择数据库", available_dbs, index=db_index, key="db_selector_simple")
    st.session_state['prev_db_simple'] = selected_db

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
    
    providers = ["openai", "gemini", "ollama", "deepseek"]
    prev_provider = st.session_state.get('prev_provider_simple', "openai")
    try:
        provider_index = providers.index(prev_provider)
    except ValueError:
        provider_index = 0
        
    model_provider = st.sidebar.selectbox(
        "选择供应商 (Provider)",
        providers,
        index=provider_index,
        key="provider_selector_simple"
    )
    st.session_state['prev_provider_simple'] = model_provider
    
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
        
    prev_model = st.session_state.get('prev_model_simple', model_options[0] if model_options else None)
    try:
        model_index = model_options.index(prev_model)
    except ValueError:
        model_index = 0
        
    selected_model = st.sidebar.selectbox("选择模型 (Model)", model_options, index=model_index, key="model_selector_simple")
    st.session_state['prev_model_simple'] = selected_model
    
    # 4. QA Selection
    st.sidebar.markdown("---")
    qa_list = load_qa_data(selected_dataset, selected_db)
    selected_qa = None
    
    # Custom Question Mode
    use_custom_q = st.sidebar.checkbox("使用自定义问题", value=False, key="use_custom_q_simple")
    
    if use_custom_q:
        custom_q = st.sidebar.text_area("输入问题", "What is the total number of schools?")
        selected_qa = {"question": custom_q, "sql_query": "-- Custom Question (No GT)", "evidence": ""}
    elif qa_list:
        qa_options = {i: f"{i}. {q['question'][:50]}..." for i, q in enumerate(qa_list)}
        prev_qa_index = st.session_state.get('prev_qa_index_simple', 0)
        if prev_qa_index >= len(qa_list):
            prev_qa_index = 0
            
        selected_index = st.sidebar.selectbox(
            "3. 选择测试问题", 
            options=qa_options.keys(), 
            format_func=lambda x: qa_options[x],
            index=prev_qa_index,
            key="qa_selector_simple"
        )
        st.session_state['prev_qa_index_simple'] = selected_index
        selected_qa = qa_list[selected_index]
    else:
        st.sidebar.warning("未找到该数据库的测试问题")

    # Show Columns Toggle
    st.sidebar.markdown("---")
    show_columns = st.sidebar.checkbox("显示图中的列节点", value=True, key="show_cols_simple")
        
    return selected_dataset, selected_db, selected_qa, model_provider, selected_model, pkl_file, show_columns

def display_prompt_response(step_name, prompts, raw_response, result_summary=None):
    """Helper to display prompts and responses in expanders"""
    st.markdown(f"#### 📝 {step_name} Details")
    
    with st.expander(f"查看 {step_name} Prompts", expanded=False):
        if isinstance(prompts, dict):
            for role, content in prompts.items():
                st.markdown(f"**{role.upper()}:**")
                st.code(content, language="markdown")
        else:
            st.code(prompts, language="markdown")
            
    with st.expander(f"查看 {step_name} Raw Response", expanded=False):
        st.code(raw_response, language="markdown")
        
    if result_summary:
        st.info(f"**Result:** {result_summary}")

# ==========================================
# 3. Main Logic
# ==========================================
def main():
    st.set_page_config(layout="wide")
    st.title("⚡ Simple NL2SQL Pipeline")
    
    # Render Sidebar
    (selected_dataset, selected_db, selected_qa, model_provider, selected_model, pkl_file, show_columns) = render_sidebar()
    
    if not selected_db or not selected_qa:
        st.info("👈 请在左侧选择完整的数据集、数据库和问题以开始")
        return

    # --- Display Question ---
    st.subheader("📋 任务详情")
    col1, col2 = st.columns([1, 1])
    with col1:
        st.info(f"**Question:** {selected_qa['question']}")
        if selected_qa.get('evidence'):
            st.warning(f"**Evidence:** {selected_qa['evidence']}")
    with col2:
        st.code(selected_qa.get('sql_query', ''), language="sql")
        st.caption("Ground Truth SQL")
        
        # Parse GT entities
        gt_entities, gt_flat_tables, gt_flat_columns = extract_gt_entities(selected_dataset, selected_db, selected_qa.get('sql_query', ''))
        gt_all_nodes = gt_flat_tables + gt_flat_columns
        with st.expander("查看 Ground Truth Schema Items"):
             st.json(gt_entities)

    st.markdown("---")

    # --- Initial Graph Visualization ---
    st.subheader("🕸️ 数据库图结构 (Schema Graph)")
    
    # Retrieve previous results from session state if available
    session_key = f"simple_sql_res_{selected_db}_{selected_qa.get('question')[:10]}"
    prev_result = st.session_state.get(session_key, {})
    
    pred_nodes = set()
    if prev_result and "selected_columns" in prev_result:
        for table, cols in prev_result["selected_columns"].items():
            pred_nodes.add(table)
            for col in cols:
                if col in ["*", ""]: continue
                if "." in col:
                    pred_nodes.add(col)
                else:
                    pred_nodes.add(f"{table}.{col}")
    
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

    # --- Run Button ---
    if st.button("🚀 运行 Simple SQL Pipeline", type="primary"):
        try:
            with st.spinner("正在初始化 Pipeline..."):
                pipeline = SimpleSQLPipeline(
                    db_name=selected_db,
                    dataset=selected_dataset,
                    provider=model_provider,
                    model=selected_model
                )
            
            # Step 1: Schema Linking
            with st.status("正在执行 Step 1: Schema Linking...", expanded=True) as status:
                status.write("正在生成 Brief Schema...")
                brief_schema = pipeline.generate_brief_schema()
                
                status.write("正在请求 LLM 进行子图选择...")
                sl_result = pipeline.schema_linking(selected_qa['question'])
                selected_columns = sl_result["selected_columns"]
                
                status.write(f"✅ 选定子图: {len(selected_columns)} tables")
                st.write("**Parsed Schema Links:**")
                st.json(selected_columns)
            
            # Display SL Prompts & Response
            display_prompt_response(
                "Schema Linking", 
                sl_result.get("prompts"), 
                sl_result.get("raw_response"),
                result_summary=f"Reasoning: {sl_result.get('reasoning')[:200]}..."
            )
            
            if not selected_columns:
                st.error("未选中任何表，流程终止。")
                return

            # Save result for graph re-render
            st.session_state[session_key] = {
                "selected_columns": selected_columns
            }
            # Rerun to update graph
            # st.rerun() 
            # Note: Rerun inside button click might clear output below? 
            # Actually, standard Streamlit flow is to show results below. 
            # To update the graph above, we need a rerun. 
            # But if we rerun, we lose the "Run" state unless we persist everything.
            # For now, let's show the results below, and the user can see the graph updated on next interaction 
            # OR we render a second "Result Graph" below (but user asked for initialization graph).
            # Let's rely on the user seeing the updated graph if they change something else or we can just render the results.
            # User requirement: "页面初始化图结构可视化展示" -> Done above.
            
            # Step 2: Full Schema & SQL Generation
            with st.status("正在执行 Step 2: SQL Generation...", expanded=True) as status:
                status.write("正在生成 Full Schema (for selected subgraph)...")
                full_schema = pipeline.generate_full_schema_for_selected(selected_columns)
                
                status.write("正在生成 SQL...")
                sql_result = pipeline.generate_sql(selected_qa['question'], selected_columns)
                sql = sql_result["sql"]
                status.write("✅ SQL 生成完成")
            
            # Display SQL Prompts & Response
            display_prompt_response(
                "SQL Generation",
                sql_result.get("prompts"),
                sql_result.get("raw_response")
            )
            
            st.subheader("🎉 生成结果")
            col_res1, col_res2 = st.columns(2)
            with col_res1:
                st.markdown("**Generated SQL:**")
                st.code(sql, language="sql")
            with col_res2:
                st.markdown("**Ground Truth SQL:**")
                st.code(selected_qa.get('sql_query', ''), language="sql")
            
        except Exception as e:
            st.error(f"运行出错: {e}")
            import traceback
            st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
