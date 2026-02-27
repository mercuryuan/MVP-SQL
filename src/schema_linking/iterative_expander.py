import json
import logging
import re
import networkx as nx
from typing import List, Dict, Any, Tuple, Set
from src.utils.schema_generator import SchemaGenerator
from src.llm.clients import LLMClient
from src.llm.prompt_manager import PromptManager

logger = logging.getLogger(__name__)

class IterativeSubgraphExpander:
    """
    Iterative Subgraph Expander (SL2)
    ---------------------------------
    Starts from an initial set of tables (from SL1) and iteratively expands the subgraph
    by examining 1-hop neighbors (Frontier) until the question is solvable.
    """

    def __init__(self, graph: nx.DiGraph, provider: str = "deepseek", model: str = None):
        self.graph = graph
        self.undirected_graph = graph.to_undirected()
        self.schema_generator = SchemaGenerator(graph)
        self.llm_client = LLMClient(provider=provider, model=model)
        self.prompt_manager = PromptManager()
        self.prompt_manager.reload() # Force reload prompts
        self.max_iterations = 3

    def _get_frontier_tables(self, core_tables: List[str]) -> List[str]:
        """Identify 1-hop neighbor tables that are not in the core."""
        frontier = set()
        for node in core_tables:
            if node in self.undirected_graph:
                neighbors = list(self.undirected_graph.neighbors(node))
                for neighbor in neighbors:
                    # Check if neighbor is a Table (not a Column)
                    # Assuming nodes with type 'Table' are tables
                    if self.graph.nodes[neighbor].get("type") == "Table":
                        if neighbor not in core_tables:
                            frontier.add(neighbor)
        return list(frontier)

    def _generate_prompt(self, question: str, core_tables: List[str], frontier_tables: List[str], iteration: int, last_reasoning: str) -> str:
        # Generate Core Schema (Full)
        core_schema_str = ""
        for table in core_tables:
            # Pass selected_tables=core_tables to show relationships within the core
            core_schema_str += self.schema_generator.generate_combined_description(table, detail_level="full", selected_tables=core_tables) + "\n"
        
        # Generate Frontier Schema (Brief)
        frontier_schema_str = ""
        if not frontier_tables:
            frontier_schema_str = "No reachable neighbor tables."
        else:
            for table in frontier_tables:
                frontier_schema_str += self.schema_generator.generate_combined_description(table, detail_level="brief") + "\n"

        iteration_context = f"Iteration: {iteration}/{self.max_iterations}\nPrevious Reasoning: {last_reasoning}"

        user_prompt = self.prompt_manager.get_prompt(
            "schema_expansion_user",
            question=question,
            iteration_context=iteration_context,
            core_schema=core_schema_str,
            frontier_schema=frontier_schema_str
        )
        return user_prompt

    def _extract_json(self, text: str) -> Dict:
        """Robust JSON extraction logic."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        
        # Markdown code block
        match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
            
        # Raw decode (handles trailing text)
        start_idx = text.find('{')
        if start_idx != -1:
            try:
                obj, _ = json.JSONDecoder().raw_decode(text[start_idx:])
                return obj
            except json.JSONDecodeError:
                pass
        
        logger.error(f"Failed to extract JSON from response: {text[:100]}...")
        return {}

    def run_expansion(self, question: str, initial_tables: List[str]) -> Dict:
        """
        Execute the iterative expansion process.
        
        Args:
            question: The natural language question.
            initial_tables: List of table names from SL1.
            
        Returns:
            Dict containing the final subgraph nodes, iteration logs, and status.
        """
        current_core = list(set(initial_tables)) # Dedup
        iteration_logs = []
        final_columns = {}
        
        last_reasoning = "Initial start from SL1 anchors."
        
        # Validation: Ensure initial tables exist in graph
        current_core = [t for t in current_core if t in self.graph.nodes]
        
        for i in range(1, self.max_iterations + 1):
            logger.info(f"--- SL2 Iteration {i} ---")
            logger.info(f"Current Core: {current_core}")
            
            frontier = self._get_frontier_tables(current_core)
            logger.info(f"Frontier: {frontier}")
            
            user_msg = self._generate_prompt(question, current_core, frontier, i, last_reasoning)
            system_msg = self.prompt_manager.get_prompt("schema_expansion_system")
            
            messages = [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ]
            
            try:
                response_text = self.llm_client.driver.request(messages)
                result = self._extract_json(response_text)
            except Exception as e:
                logger.error(f"LLM Error in iteration {i}: {e}")
                result = {}
                response_text = str(e)
            
            log_entry = {
                "iteration": i,
                "core_tables": list(current_core),
                "frontier_tables": list(frontier),
                "prompts": {
                    "system": system_msg,
                    "user": user_msg
                },
                "llm_response": result,
                "raw_response": response_text
            }
            iteration_logs.append(log_entry)
            
            # Parse actions
            selected_cols = result.get("selected_columns_from_core", {})
            # Merge columns
            for table, cols in selected_cols.items():
                if table not in final_columns:
                    final_columns[table] = set()
                # Handle case where cols might be a string or list
                if isinstance(cols, list):
                    final_columns[table].update(cols)
                elif isinstance(cols, str):
                    final_columns[table].add(cols)
                
            selected_frontier = result.get("selected_tables_from_frontier", [])
            solvable_info = result.get("to_solve_the_question", {})
            is_solvable = solvable_info.get("is_solvable", False)
            last_reasoning = solvable_info.get("reasoning", "")
            
            if is_solvable:
                logger.info("LLM deems question solvable. Stopping.")
                break
                
            # Expand
            if not selected_frontier:
                logger.info("No new tables selected from frontier. Stopping to avoid deadlock.")
                break
            
            # Add valid frontier tables to core
            valid_frontier = [t for t in selected_frontier if t in frontier]
            if not valid_frontier:
                 logger.info("Selected frontier tables are not valid or not in frontier. Stopping.")
                 break
                 
            current_core.extend(valid_frontier)
            current_core = list(set(current_core))
            
        # Final construction
        # 1. Tables
        final_nodes = set(current_core)
        
        # 2. Columns
        for table, cols in final_columns.items():
            for col in cols:
                # Assuming column node naming convention is "Table.Column" or just "Column"
                # Check graph to be sure. Based on SchemaGenerator, it seems columns are nodes.
                # We need to construct the node ID. Usually "Table.Column" or just the name if unique.
                # Let's try to find the column node in the graph connected to the table.
                
                # Try "Table.Col" first
                candidate_1 = f"{table}.{col}"
                if candidate_1 in self.graph.nodes:
                    final_nodes.add(candidate_1)
                    continue
                    
                # Try finding by edge
                found = False
                if table in self.graph:
                    for neighbor in self.graph.successors(table):
                        # Assuming HAS_COLUMN edge
                        if self.graph.nodes[neighbor].get("name") == col:
                            final_nodes.add(neighbor)
                            found = True
                            break
                if not found:
                    logger.warning(f"Could not find node for column {col} in table {table}")

        return {
            "final_core_tables": current_core,
            "final_selected_columns": {k: list(v) for k, v in final_columns.items()},
            "final_subgraph_nodes": list(final_nodes),
            "iterations": iteration_logs,
            "status": "completed"
        }
