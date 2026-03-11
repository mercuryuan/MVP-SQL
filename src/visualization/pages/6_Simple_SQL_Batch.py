import streamlit as st
import sys
import os
import json
from pathlib import Path
from datetime import datetime
import logging

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from configs import paths
from src.utils.dataloder import DataLoader
from src.llm.clients import LLMClient
from src.simple_sql.pipeline import SimpleSQLPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")

def get_subdirs(path):
    if not os.path.exists(path):
        return []
    return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])

def list_dbs_for_dataset(dataset_name):
    dataset_path = os.path.join(ROOT_DIR, dataset_name)
    return get_subdirs(dataset_path)

def list_simple_batches(dataset_name):
    base = Path(project_root) / "output" / "simple" / dataset_name
    if not base.exists():
        return []
    return sorted([p.name for p in base.iterdir() if p.is_dir()], reverse=True)

def map_dataset_split_for_loader(dataset_name, split):
    if dataset_name == "spider":
        return "spider_dev" if split == "dev" else "spider"
    if dataset_name == "bird":
        return "bird_dev" if split == "dev" else "bird"
    return dataset_name

def render_sidebar():
    st.sidebar.header("🧪 批量运行配置 (Simple SQL)")
    available_datasets = get_subdirs(ROOT_DIR)
    if not available_datasets:
        st.sidebar.error(f"未发现数据集目录: {ROOT_DIR}")
        return None, None, None, None, None, None, None, None, None

    prev_dataset = st.session_state.get('batch_prev_dataset', available_datasets[0])
    try:
        dataset_index = available_datasets.index(prev_dataset)
    except ValueError:
        dataset_index = 0
    selected_dataset = st.sidebar.selectbox("1. 选择数据集", available_datasets, index=dataset_index, key="batch_dataset_selector")
    st.session_state['batch_prev_dataset'] = selected_dataset

    split_options = ["dev", "train"]
    prev_split = st.session_state.get('batch_prev_split', "dev")
    try:
        split_index = split_options.index(prev_split)
    except ValueError:
        split_index = 0
    selected_split = st.sidebar.selectbox("2. 选择数据划分", split_options, index=split_index, key="batch_split_selector")
    st.session_state['batch_prev_split'] = selected_split
    
    spider_subset = None
    if selected_dataset == "spider" and selected_split == "train":
        subset_options = ["train_all", "train_spider", "train_others"]
        prev_subset = st.session_state.get('batch_spider_train_subset', "train_all")
        try:
            subset_index = subset_options.index(prev_subset)
        except ValueError:
            subset_index = 0
        spider_subset = st.sidebar.radio("Spider 训练集来源", subset_options, index=subset_index, key="batch_spider_train_subset", horizontal=True)

    dbs = list_dbs_for_dataset(selected_dataset)
    if not dbs:
        st.sidebar.warning("该数据集未发现数据库")
        return selected_dataset, selected_split, [], None, None, None, None, None

    prev_selected_dbs = st.session_state.get('batch_prev_dbs', dbs[:1])
    default_dbs = [d for d in prev_selected_dbs if d in dbs] or dbs[:1]
    selected_dbs = st.sidebar.multiselect("3. 多选数据库", dbs, default=default_dbs, key="batch_db_selector")
    st.session_state['batch_prev_dbs'] = selected_dbs

    st.sidebar.markdown("---")
    st.sidebar.subheader("🤖 模型配置")
    providers = ["openai", "gemini", "ollama", "deepseek"]
    prev_provider = st.session_state.get('batch_prev_provider', "openai")
    try:
        provider_index = providers.index(prev_provider)
    except ValueError:
        provider_index = 0
    model_provider = st.sidebar.selectbox("选择供应商 (Provider)", providers, index=provider_index, key="batch_provider_selector")
    st.session_state['batch_prev_provider'] = model_provider

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
            model_options = ollama_models if ollama_models else ["llama3", "mistral"]
        except Exception:
            model_options = ["llama3", "mistral"]

    prev_model = st.session_state.get('batch_prev_model', model_options[0] if model_options else None)
    try:
        model_index = model_options.index(prev_model)
    except ValueError:
        model_index = 0
    selected_model = st.sidebar.selectbox("选择模型 (Model)", model_options, index=model_index, key="batch_model_selector")
    st.session_state['batch_prev_model'] = selected_model

    st.sidebar.markdown("---")
    range_enabled = st.sidebar.checkbox("设置评估问题ID范围", value=False, key="batch_id_range_enabled")
    id_start, id_end = None, None
    if range_enabled and selected_dbs:
        # 计算建议的 ID 范围：对所选数据库聚合
        try:
            loader_key = map_dataset_split_for_loader(selected_dataset, selected_split)
            loader = DataLoader(loader_key)
            ids = []
            for i, item in enumerate(loader.data):
                if item.get("db_id") in selected_dbs:
                    used_id = item.get("question_id", i)
                    if isinstance(used_id, int):
                        ids.append(used_id)
            if ids:
                min_id, max_id = min(ids), max(ids)
                id_start = st.sidebar.number_input("起始ID", min_value=min_id, max_value=max_id, value=min_id, step=1, key="batch_id_start")
                id_end = st.sidebar.number_input("结束ID", min_value=id_start, max_value=max_id, value=max_id, step=1, key="batch_id_end")
        except Exception:
            st.sidebar.warning("无法计算建议ID范围，仍可手动设置。")
            id_start = st.sidebar.number_input("起始ID", value=0, step=1, key="batch_id_start")
            id_end = st.sidebar.number_input("结束ID", value=id_start, step=1, key="batch_id_end")

    return selected_dataset, selected_split, selected_dbs, model_provider, selected_model, (id_start if range_enabled else None), (id_end if range_enabled else None), spider_subset

def main():
    st.set_page_config(layout="wide")
    st.title("📦 Simple NL2SQL 批量运行")
    st.caption("选择数据集与多个数据库，批量执行两阶段推理，并保存到 output/simple/时间 目录")

    selected_dataset, selected_split, selected_dbs, model_provider, selected_model, id_start, id_end, spider_subset = render_sidebar()
    if not selected_dbs or not selected_dataset:
        st.info("👈 请在左侧完成基本配置")
        return

    loader_key = map_dataset_split_for_loader(selected_dataset, selected_split)
    if selected_dataset == "spider" and selected_split == "train":
        if spider_subset == "train_spider":
            loader_key = "spider_train"
        elif spider_subset == "train_others":
            loader_key = "spider_other"

    st.markdown("---")
    st.subheader("结果浏览")
    batches = list_simple_batches(selected_dataset)
    if batches:
        prev = st.session_state.get('batch_selected_batch')
        if (prev is None) or (prev not in batches):
            st.session_state['batch_selected_batch'] = batches[0]
    selected_batch = st.selectbox("选择结果批次", batches, key="batch_selected_batch") if batches else None
    view_type_index = 0 if st.session_state.get('batch_view_type', '总体') == '总体' else 1
    view_type = st.radio("选择展示类型", ["总体", "详细"], index=view_type_index, horizontal=True, key="batch_view_type")
    if selected_batch:
        base_path = Path(project_root) / "output" / "simple" / selected_dataset / selected_batch
        fp = base_path / ("summary.json" if view_type == "总体" else "details.json")
        if fp.exists():
            with open(fp, "r", encoding="utf-8") as f:
                st.json(json.load(f))
        else:
            st.warning("该批次结果文件不存在")
    st.markdown("---")
    run_col1, run_col2 = st.columns([1, 1])
    with run_col1:
        start = st.button("🚀 一键批量运行", type="primary")
    with run_col2:
        st.write(f"输出目录根: {Path(project_root) / 'output' / 'simple' / selected_dataset}")

    if not start:
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if selected_dataset == "bird":
        suffix = "bird_dev" if selected_split == "dev" else "bird_train"
    else:
        if selected_split == "dev":
            suffix = "spider_dev"
        else:
            if spider_subset == "train_spider":
                suffix = "spider_train_spider"
            elif spider_subset == "train_others":
                suffix = "spider_train_others"
            else:
                suffix = "spider_train_all"
    output_root = Path(project_root) / "output" / "simple" / selected_dataset / f"{ts}_{suffix}"
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "summary.json"
    details_path = output_root / "details.json"

    if os.path.exists(summary_path):
        with open(summary_path, "r", encoding="utf-8") as f:
            overall_summary = json.load(f)
    else:
        overall_summary = {
            "dataset": selected_dataset,
            "split": selected_split,
            "provider": model_provider,
            "model": selected_model,
            "dbs": {}
        }
    if os.path.exists(details_path):
        with open(details_path, "r", encoding="utf-8") as f:
            overall_details = json.load(f)
    else:
        overall_details = {
            "dataset": selected_dataset,
            "split": selected_split,
            "provider": model_provider,
            "model": selected_model,
            "dbs": {}
        }

    progress = st.progress(0.0)
    status = st.empty()
    total_tasks = sum(1 for _ in selected_dbs)
    done_tasks = 0

    try:
        for db_id in selected_dbs:
            status.info(f"初始化 {db_id} ...")
            pipeline = SimpleSQLPipeline(
                db_name=db_id,
                dataset=selected_dataset,
                provider=model_provider,
                model=selected_model
            )
            loader = DataLoader(loader_key)
            # 原始索引列表与过滤后数据一一对应
            pos_list = [i for i, it in enumerate(loader.data) if it.get("db_id") == db_id]
            qa_list = loader.filter_data(db_id=db_id, fields=["question", "sql_query", "evidence"])
            paired = []
            for j, qa in enumerate(qa_list):
                orig_item = loader.data[pos_list[j]]
                qid = orig_item.get("question_id", pos_list[j])
                if id_start is not None and id_end is not None:
                    if not (id_start <= qid <= id_end):
                        continue
                paired.append((qid, qa))

        if db_id not in overall_summary["dbs"]:
            overall_summary["dbs"][db_id] = []
        if db_id not in overall_details["dbs"]:
            overall_details["dbs"][db_id] = {}

            for qid, qa in paired:
                q = qa.get("question", "")
                if str(qid) in overall_details["dbs"][db_id]:
                    continue
                if any((e.get("question_id") == qid) for e in overall_summary["dbs"][db_id]):
                    continue
                sl_result = None
                selected_columns = {}
                retry_sl = 0
                for attempt in range(1, 4):
                    try:
                        r = pipeline.schema_linking(q)
                        sc = r.get("selected_columns", {})
                        if not isinstance(sc, dict) or len(sc) == 0:
                            raise ValueError("schema_linking returned empty selected_columns")
                        sl_result = r
                        selected_columns = sc
                        retry_sl = attempt
                        break
                    except Exception as e:
                        status.warning(f"重试 Schema Linking 第{attempt}/3次: db={db_id}, qid={qid}")
                        logger.warning(f"RETRY Schema Linking {attempt}/3 db={db_id} qid={qid}: {e}")
                if sl_result is None:
                    overall_details["dbs"][db_id][str(qid)] = {
                        "question": q,
                        "sql_query": qa.get("sql_query"),
                        "evidence": qa.get("evidence"),
                        "question_id": qid,
                        "error": "schema_linking_failed",
                        "retry_attempts": 3
                    }
                    with open(details_path, "w", encoding="utf-8") as f:
                        json.dump(overall_details, f, ensure_ascii=False, indent=2)
                    continue
                sql_result = None
                sql_text = ""
                retry_sql = 0
                for attempt in range(1, 4):
                    try:
                        r = pipeline.generate_sql(q, selected_columns)
                        s = r.get("sql", "")
                        if not isinstance(s, str) or len(s.strip()) == 0 or s.strip() == "-- No tables selected.":
                            raise ValueError("sql_generation returned empty sql")
                        sql_result = r
                        sql_text = s
                        retry_sql = attempt
                        break
                    except Exception as e:
                        status.warning(f"重试 SQL Generation 第{attempt}/3次: db={db_id}, qid={qid}")
                        logger.warning(f"RETRY SQL Generation {attempt}/3 db={db_id} qid={qid}: {e}")
                if sql_result is None:
                    overall_details["dbs"][db_id][str(qid)] = {
                        "question": q,
                        "sql_query": qa.get("sql_query"),
                        "evidence": qa.get("evidence"),
                        "question_id": qid,
                        "schema_linking": sl_result,
                        "error": "sql_generation_failed",
                        "retry_attempts": 3
                    }
                    with open(details_path, "w", encoding="utf-8") as f:
                        json.dump(overall_details, f, ensure_ascii=False, indent=2)
                    continue

                overall_summary["dbs"][db_id].append({
                    "question_id": qid,
                    "question": q,
                    "selected_columns": selected_columns,
                    "sql": sql_text,
                    **({"retry_schema_linking": retry_sl} if retry_sl > 1 else {}),
                    **({"retry_sql_generation": retry_sql} if retry_sql > 1 else {})
                })

                overall_details["dbs"][db_id][str(qid)] = {
                    "question": q,
                    "sql_query": qa.get("sql_query"),
                    "evidence": qa.get("evidence"),
                    "question_id": qid,
                    "schema_linking": sl_result,
                    "sql_generation": sql_result,
                    "retry_schema_linking": retry_sl,
                    "retry_sql_generation": retry_sql
                }
                with open(summary_path, "w", encoding="utf-8") as f:
                    json.dump(overall_summary, f, ensure_ascii=False, indent=2)
                with open(details_path, "w", encoding="utf-8") as f:
                    json.dump(overall_details, f, ensure_ascii=False, indent=2)

            done_tasks += 1
            progress.progress(done_tasks / total_tasks if total_tasks > 0 else 1.0)

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(overall_summary, f, ensure_ascii=False, indent=2)
        with open(details_path, "w", encoding="utf-8") as f:
            json.dump(overall_details, f, ensure_ascii=False, indent=2)

        st.success("批量运行完成，结果已保存")
        st.markdown(f"**保存目录**: {output_root}")

        st.json({"summary_path": str(summary_path), "details_path": str(details_path)})
        st.session_state['batch_selected_batch'] = output_root.name
        st.rerun()
    except Exception as e:
        st.error(f"批量运行出错: {e}")
        import traceback
        st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
