import sys
import os
import json
from pathlib import Path
from datetime import datetime
import argparse
import logging

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from configs import paths
from src.utils.dataloder import DataLoader
from src.simple_sql.pipeline import SimpleSQLPipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ROOT_DIR = os.path.join(paths.OUTPUT_ROOT, "schema_graph_repo")
try:
    from tqdm import tqdm
    _use_tqdm = True
except Exception:
    _use_tqdm = False

def get_subdirs(path):
    if not os.path.exists(path):
        return []
    return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])

def list_dbs_for_dataset(dataset_name):
    dataset_path = os.path.join(ROOT_DIR, dataset_name)
    return get_subdirs(dataset_path)

def map_dataset_split_for_loader(dataset_name, split, spider_subset=None):
    if dataset_name == "spider":
        if split == "dev":
            return "spider_dev"
        if spider_subset == "train_spider":
            return "spider_train"
        if spider_subset == "train_others":
            return "spider_other"
        return "spider"
    if dataset_name == "bird":
        return "bird_dev" if split == "dev" else "bird"
    return dataset_name

def suffix_for_run(dataset_name, split, spider_subset=None):
    if dataset_name == "bird":
        return "bird_dev" if split == "dev" else "bird_train"
    if split == "dev":
        return "spider_dev"
    if spider_subset == "train_spider":
        return "spider_train_spider"
    if spider_subset == "train_others":
        return "spider_train_others"
    return "spider_train_all"

def run_batch(dataset, split, dbs, provider, model, id_start=None, id_end=None, spider_subset=None, output_dir=None, max_retries=3):
    loader_key = map_dataset_split_for_loader(dataset, split, spider_subset)
    if output_dir:
        output_root = Path(output_dir)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_root = project_root / "output" / "simple" / dataset / f"{ts}_{suffix_for_run(dataset, split, spider_subset)}"
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "summary.json"
    details_path = output_root / "details.json"

    if os.path.exists(summary_path):
        with open(summary_path, "r", encoding="utf-8") as f:
            overall_summary = json.load(f)
    else:
        overall_summary = {
            "dataset": dataset,
            "split": split,
            "provider": provider,
            "model": model,
            "dbs": {}
        }
    if os.path.exists(details_path):
        with open(details_path, "r", encoding="utf-8") as f:
            overall_details = json.load(f)
    else:
        overall_details = {
            "dataset": dataset,
            "split": split,
            "provider": provider,
            "model": model,
            "dbs": {}
        }

    db_iter = tqdm(dbs, desc="Databases", unit="db") if _use_tqdm else dbs
    for db_id in db_iter:
        pipeline = SimpleSQLPipeline(db_name=db_id, dataset=dataset, provider=provider, model=model)
        loader = DataLoader(loader_key)
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

        q_iter = tqdm(paired, desc=db_id, unit="q", leave=False) if _use_tqdm else paired
        for qid, qa in paired:
            if str(qid) in overall_details["dbs"][db_id]:
                continue
            if any((e.get("question_id") == qid) for e in overall_summary["dbs"][db_id]):
                continue
            q = qa.get("question", "")
            sl_result = None
            selected_columns = {}
            retry_sl = 0
            for attempt in range(1, max_retries + 1):
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
                    logger.warning(f"[RETRY] Schema Linking {attempt}/{max_retries} db={db_id} qid={qid}: {e}")
            if sl_result is None:
                overall_details["dbs"][db_id][str(qid)] = {
                    "question": q,
                    "sql_query": qa.get("sql_query"),
                    "evidence": qa.get("evidence"),
                    "question_id": qid,
                    "error": "schema_linking_failed",
                    "retry_attempts": max_retries
                }
                with open(details_path, "w", encoding="utf-8") as f:
                    json.dump(overall_details, f, ensure_ascii=False, indent=2)
                continue

            sql_result = None
            sql_text = ""
            retry_sql = 0
            for attempt in range(1, max_retries + 1):
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
                    logger.warning(f"[RETRY] SQL Generation {attempt}/{max_retries} db={db_id} qid={qid}: {e}")
            if sql_result is None:
                overall_details["dbs"][db_id][str(qid)] = {
                    "question": q,
                    "sql_query": qa.get("sql_query"),
                    "evidence": qa.get("evidence"),
                    "question_id": qid,
                    "schema_linking": sl_result,
                    "error": "sql_generation_failed",
                    "retry_attempts": max_retries
                }
                with open(details_path, "w", encoding="utf-8") as f:
                    json.dump(overall_details, f, ensure_ascii=False, indent=2)
                continue

            entry = {
                "question_id": qid,
                "question": q,
                "selected_columns": selected_columns,
                "sql": sql_text
            }
            if retry_sl > 1:
                entry["retry_schema_linking"] = retry_sl
            if retry_sql > 1:
                entry["retry_sql_generation"] = retry_sql
            overall_summary["dbs"][db_id].append(entry)

            overall_details["dbs"][db_id][str(qid)] = {
                "question": q,
                "sql_query": qa.get("sql_query"),
                "evidence": qa.get("evidence"),
                "question_id": qid,
                "schema_linking": sl_result,
                "sql_generation": sql_result,
            }
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(overall_summary, f, ensure_ascii=False, indent=2)
            with open(details_path, "w", encoding="utf-8") as f:
                json.dump(overall_details, f, ensure_ascii=False, indent=2)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(overall_summary, f, ensure_ascii=False, indent=2)
    with open(details_path, "w", encoding="utf-8") as f:
        json.dump(overall_details, f, ensure_ascii=False, indent=2)
    print(json.dumps({"summary_path": str(summary_path), "details_path": str(details_path)}))

def main():
    parser = argparse.ArgumentParser(description="Batch run Simple NL2SQL on Linux")
    parser.add_argument("--dataset", required=True, choices=["bird", "spider"])
    parser.add_argument("--split", required=True, choices=["dev", "train"])
    parser.add_argument("--spider-subset", choices=["train_all", "train_spider", "train_others"])
    parser.add_argument("--provider", required=True, choices=["openai", "gemini", "ollama", "deepseek"])
    parser.add_argument("--model", required=True)
    parser.add_argument("--dbs", help="Comma-separated db ids; if omitted, use all dbs")
    parser.add_argument("--id-start", type=int)
    parser.add_argument("--id-end", type=int)
    parser.add_argument("--output-dir")
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args()

    if args.dbs:
        dbs = [x.strip() for x in args.dbs.split(",") if x.strip()]
    else:
        dbs = list_dbs_for_dataset(args.dataset)

    run_batch(
        dataset=args.dataset,
        split=args.split,
        dbs=dbs,
        provider=args.provider,
        model=args.model,
        id_start=args.id_start,
        id_end=args.id_end,
        spider_subset=args.spider_subset,
        output_dir=args.output_dir,
        max_retries=args.max_retries
    )

if __name__ == "__main__":
    main()
    # python src/simple_sql/batch_cli.py --dataset spider  --split dev --provider ollama  --model qwen3:1.7b
    # python src/simple_sql/batch_cli.py --dataset bird  --split dev --provider ollama  --model ds-coder
