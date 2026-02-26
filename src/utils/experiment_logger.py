import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# 导入路径配置
from configs.paths import INFERENCE_RESULT_ROOT

class ExperimentLogger:
    """
    实验结果记录器 (Experiment Logger)
    用于独立于 UI 存储和读取模型推理结果、评估指标等。
    实现“一套代码跑通所有数据集”的持久化层。
    """

    def __init__(self, dataset_name: str, task_name: str = "schema_linking"):
        """
        初始化记录器
        :param dataset_name: 数据集名称 (如 'spider', 'bird')
        :param task_name: 任务名称 (如 'schema_linking', 'text2sql')
        """
        self.dataset_name = dataset_name
        self.task_name = task_name
        
        # 结果根目录: data/inference_results / [task_name] / [dataset_name]
        self.base_path = Path(INFERENCE_RESULT_ROOT) / task_name / self.dataset_name
        self.base_path.mkdir(parents=True, exist_ok=True)

    def get_db_result_path(self, db_id: str) -> Path:
        """获取特定数据库的结果文件路径"""
        return self.base_path / f"{db_id}.json"

    def load_results(self, db_id: str) -> Dict[str, Any]:
        """
        加载特定数据库的所有历史结果
        结构兼容 src/visualization/pages/3_Schema_Linking.py
        """
        path = self.get_db_result_path(db_id)
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                # 使用 print 或标准 logging，不依赖 UI 组件
                print(f"Error loading results from {path}: {e}")
        return {}

    def save_result(
        self, 
        db_id: str, 
        question_index: int, 
        question: str, 
        model_key: str, 
        result_data: Dict[str, Any],
        ground_truth_sql: Optional[str] = None,
        evidence: Optional[str] = None
    ) -> bool:
        """
        保存单次推理结果
        
        结构说明:
        {
            db_id: {
                "question_index": {
                    "question": "...",
                    "sql_query": "...",  # 对应数据集中的 ground truth sql
                    "evidence": "...",   # 对应数据集中的 external knowledge (BIRD)
                    "schema_linking_results": {  # 保持与 UI 兼容的字段名
                        "model_key": {
                            "result": {...},
                            "timestamp": "..."
                        }
                    }
                }
            }
        }
        """
        path = self.get_db_result_path(db_id)
        all_results = self.load_results(db_id)
        
        if db_id not in all_results:
            all_results[db_id] = {}
            
        q_idx_str = str(question_index)
        if q_idx_str not in all_results[db_id]:
            all_results[db_id][q_idx_str] = {
                "question": question,
                "sql_query": ground_truth_sql,
                "evidence": evidence,
                "schema_linking_results": {}
            }
        else:
            # Update meta info if needed (e.g. if logging from a different source that has more info)
            if ground_truth_sql:
                all_results[db_id][q_idx_str]["sql_query"] = ground_truth_sql
            if evidence:
                all_results[db_id][q_idx_str]["evidence"] = evidence
            
        # 写入结果，使用 model_key 作为唯一标识
        all_results[db_id][q_idx_str]["schema_linking_results"][model_key] = {
            "result": result_data,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"Error saving result to {path}: {e}")
            return False

    def get_question_result(self, db_id: str, question_index: int, model_key: str) -> Optional[Dict[str, Any]]:
        """获取特定问题和特定模型的结果"""
        all_results = self.load_results(db_id)
        q_idx_str = str(question_index)
        
        try:
            return all_results[db_id][q_idx_str]["schema_linking_results"][model_key]
        except KeyError:
            return None
