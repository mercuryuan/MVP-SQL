import concurrent.futures
import json
import logging
import os
import time
from typing import Dict, Any, List, Generator

from configs import paths
from src.schema_linking_v2.sl1 import TableSelector
from src.schema_linking_v2.sl2 import SubgraphSelector
from src.schema_linking_v2.sl3 import CandidateSelector
from src.utils.graph_explorer import GraphExplorer
from src.utils.graph_loader import GraphLoader
# from src.utils.validator import Validator

logger = logging.getLogger(__name__)


class SchemaLinkingPipelineV2:
    def __init__(self, dataset_name: str, db_name: str, question_data: Dict[str, Any] = None):
        self.dataset_name = dataset_name
        self.db_name = db_name
        self.question_data = question_data or {}
        
        # Load Graph
        # Path: output/schema_graph_repo/[dataset_name]/[db_name]/[db_name].pkl
        self.pkl_path = paths.OUTPUT_ROOT / "schema_graph_repo" / dataset_name / db_name / f"{db_name}.pkl"
        
        self.graph = GraphLoader.load_graph(str(self.pkl_path))
        if self.graph is None:
            raise ValueError(f"Failed to load graph from {self.pkl_path}")
            
        self.explorer = GraphExplorer(self.graph)
        # self.validator = Validator(self.explorer)
        
        # Initialize components
        self.sl1 = TableSelector(dataset_name, db_name, self.explorer, self.question_data)
        self.sl2 = SubgraphSelector(dataset_name, db_name, self.explorer, self.question_data)
        self.sl3 = CandidateSelector(dataset_name, db_name, self.explorer, self.question_data)
        
        # Detail level configs (default)
        self.sl1_detail = "brief"
        self.sl2_detail = "brief"
        self.sl3_detail = "full"

    def run_stream(self) -> Generator[Dict[str, Any], None, Dict[str, Any]]:
        """
        Run the schema linking pipeline with streaming updates.
        Yields intermediate results.
        Returns final result.
        """
        question = self.question_data.get("question", "")
        if not question:
            logger.error("No question provided in question_data")
            yield {"step": "error", "message": "No question provided"}
            return {}

        logger.info(f"Starting Schema Linking V2 for question: {question}")
        yield {"step": "sl1_start", "message": "Step 1: Initial Table Selection (SL1)..."}
        
        # --- Step 1: Initial Table Selection (SL1) ---
        all_tables = self.explorer.get_all_tables()
        
        db_schema_sl1 = []
        for table in all_tables:
            desc = self.sl1.schema_generator.generate_combined_description(table, detail_level=self.sl1_detail)
            db_schema_sl1.append(desc)
        db_schema_sl1_str = "\n".join(db_schema_sl1)
        
        sl1_retries = 3
        sl1_result = {}
        sl1_prompts = {}
        
        for attempt in range(sl1_retries):
            try:
                sl1_result, sl1_prompts = self.sl1.select_relevant_tables(db_schema_sl1_str, question)
                # Validator Removed: Rely on LLM output directly
                # is_valid, corrected_result, error_msg = self.validator.validate_sl1(sl1_result)
                
                # Simple check: selected_entity must be a list
                selected_entity = sl1_result.get("selected_entity")
                if isinstance(selected_entity, list) and len(selected_entity) > 0:
                    break
                else:
                    logger.warning(f"SL1 attempt {attempt+1} failed: No tables selected or invalid format.")
                    if attempt == sl1_retries - 1:
                         # Last attempt failed
                         pass
            except Exception as e:
                logger.error(f"SL1 attempt {attempt+1} exception: {e}")
                if attempt == sl1_retries - 1:
                    yield {"step": "error", "message": f"SL1 failed: {e}"}
                    return {}

        selected_tables = sl1_result.get("selected_entity", [])
        value_keywords = sl1_result.get("value_keywords", [])
        
        logger.info(f"SL1 Selected tables: {selected_tables}")
        yield {"step": "sl1_complete", "result": sl1_result, "prompts": sl1_prompts}
        
        if not selected_tables:
            logger.warning("No tables selected in SL1.")
            return {"sl1_result": sl1_result}

        # --- Step 2: Subgraph Expansion (SL2) ---
        yield {"step": "sl2_start", "message": f"Step 2: Subgraph Expansion (SL2) for {len(selected_tables)} tables...", "selected_tables": selected_tables}
        
        candidate_results = {}
        
        # To support streaming in SL2, we need to handle concurrency carefully.
        # We want to yield as soon as one table is done.
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_table = {
                executor.submit(self._run_sl2_for_table, table, question, value_keywords): table 
                for table in selected_tables
            }
            
            for future in concurrent.futures.as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    sl2_table_result = future.result()
                    if sl2_table_result:
                        # Extract final result from the detailed history
                        final_res = sl2_table_result.get("final_result", {})
                        candidate_results[table] = final_res
                        
                        # Yield the full detailed result for this table
                        yield {
                            "step": "sl2_table_complete", 
                            "table": table, 
                            "result": sl2_table_result 
                        }
                except Exception as exc:
                    logger.error(f"SL2 execution for table {table} generated an exception: {exc}")
                    yield {"step": "sl2_table_error", "table": table, "error": str(exc)}

        # --- Step 3: Candidate Selection (SL3) ---
        yield {"step": "sl3_start", "message": "Step 3: Candidate Selection (SL3)..."}
        
        recommend_tables = json.dumps(sl1_result.get("reasoning", {}), indent=2)
        keyword_hints = f"Value Keywords: {value_keywords}"
        
        # Pass detail level to sl3.select_candidate if needed, but sl3.select_candidate calls generate_combined_description_for_selected
        # We need to modify sl3 to accept detail_level or modify pipeline to set it on sl3 object.
        # But sl3 is initialized in init. Let's pass it to select_candidate.
        
        final_result, is_consistent, sl3_prompts = self.sl3.select_candidate(
            candidate_results, 
            question, 
            recommend_tables, 
            keyword_hints,
            detail_level=self.sl3_detail
        )
        
        # Validator Removed
        # final_result = self.validator.validate_and_correct(final_result)
        
        # Construct final return dict
        full_result = {
            "final_result": final_result,
            "is_consistent": is_consistent,
            "sl1_result": sl1_result,
            "candidate_results": candidate_results, # Note: this is just final dicts, not full history
            "_prompts": {
                "sl1": sl1_prompts,
                "sl3": sl3_prompts
                # sl2 prompts are inside candidate_results/history
            }
        }
        
        yield {"step": "sl3_complete", "result": full_result, "prompts": sl3_prompts}
        return full_result

    def run(self) -> Dict[str, Any]:
        """Backward compatibility wrapper for run_stream"""
        final_res = {}
        for item in self.run_stream():
            if item["step"] == "sl3_complete":
                final_res = item["result"]
        return final_res

    def _run_sl2_for_table(self, start_table: str, question: str, value_keywords: List[str]) -> Dict[str, Any]:
        """
        Run SL2 iterations for a single starting table.
        Returns a dict with execution history and final result.
        """
        current_selected_tables = [start_table]
        result_from_last_round = ""
        hint = ""
        max_iterations = 5
        
        # Format keyword hints
        keyword_hints = f"Potential values: {', '.join(value_keywords)}" if value_keywords else ""
        
        history = []
        final_iteration_result = {}
        
        for i in range(max_iterations):
            try:
                # Generate schema description based on current selection (0-hop + 1-hop view)
                db_schema_sl2 = self.sl2.generate_schema_description(current_selected_tables, detail_level=self.sl2_detail)
                
                # Record Prompt Context (Optional, for debugging)
                
                result, sl2_prompts = self.sl2.select_relevant_tables(
                    db_schema=db_schema_sl2,
                    question=question,
                    select_table=current_selected_tables,
                    keyword_hints=keyword_hints,
                    result_from_last_round=result_from_last_round,
                    hint=hint
                )
                
                # Validator Removed
                # result = self.validator.validate_and_correct(result)
                
                # Store iteration info
                iteration_info = {
                    "iteration": i + 1,
                    "core_tables": list(current_selected_tables), # Copy list
                    "llm_response": result, 
                    "prompts": sl2_prompts,
                    "db_schema_context_len": len(db_schema_sl2)
                }
                history.append(iteration_info)
                
                final_iteration_result = result
                
                # Check solvability
                to_solve = result.get("to_solve_the_question", {})
                if to_solve.get("is_solvable") is True:
                    # Found a solution
                    return {
                        "table": start_table,
                        "final_result": result,
                        "history": history,
                        "status": "solved"
                    }
                
                # Prepare for next iteration
                selected_columns = result.get("selected_columns", {})
                
                # Double check if selected_columns is a dict (Validator should have handled this, but be safe)
                if not isinstance(selected_columns, dict):
                     # If validator failed to fix it, stop here
                     logger.error(f"selected_columns is not a dict: {type(selected_columns)}")
                     return {
                        "table": start_table,
                        "final_result": result,
                        "history": history,
                        "status": "format_error",
                        "error_message": "Invalid format: selected_columns must be a dictionary."
                    }

                new_selected_tables = list(selected_columns.keys())
                
                if set(new_selected_tables) == set(current_selected_tables):
                    # Stuck
                    pass
                
                current_selected_tables = new_selected_tables
                result_from_last_round = json.dumps(result)
            
            except Exception as e:
                logger.error(f"Error in SL2 iteration {i+1} for table {start_table}: {e}")
                # Append error info to history if possible
                history.append({
                    "iteration": i + 1,
                    "error": str(e),
                    "status": "exception"
                })
                return {
                    "table": start_table,
                    "final_result": final_iteration_result if final_iteration_result else {"error": str(e)},
                    "history": history,
                    "status": "error",
                    "error_message": str(e)
                }
            
        return {
            "table": start_table,
            "final_result": final_iteration_result,
            "history": history,
            "status": "max_iterations_reached"
        }

if __name__ == "__main__":
    # Test run
    logging.basicConfig(level=logging.INFO)
    try:
        pipeline = SchemaLinkingPipelineV2(
            dataset_name="bird", 
            db_name="california_schools", 
            question_data={"question": "What is the highest SAT score?"}
        )
        print("Pipeline initialized successfully.")
    except Exception as e:
        print(f"Error initializing pipeline: {e}")
