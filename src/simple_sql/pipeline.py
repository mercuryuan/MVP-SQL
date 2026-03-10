import sys
from pathlib import Path
import json
import logging
import re

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import configs.paths
from dotenv import load_dotenv

import os
# Load environment variables from src/llm/.env
env_path = project_root / "src" / "llm" / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    print(f"Warning: .env file not found at {env_path}")

from src.utils.graph_loader import GraphLoader
from src.utils.schema_generator import SchemaGenerator
from src.llm.clients import LLMClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleSQLPipeline:
    def __init__(self, db_name: str, dataset: str = "bird", provider: str = None, model: str = None):
        self.db_name = db_name
        self.dataset = dataset
        
        # Path to the serialized graph file
        # Default structure: output/schema_graph_repo/{dataset}/{db_name}/{db_name}.pkl
        self.pkl_path = Path(configs.paths.OUTPUT_ROOT) / "schema_graph_repo" / dataset / db_name / f"{db_name}.pkl"
        
        logger.info(f"Loading graph from {self.pkl_path}")
        self.graph = GraphLoader.load_graph(str(self.pkl_path))
        
        if self.graph is None:
            logger.error(f"Failed to load graph from {self.pkl_path}")
            raise FileNotFoundError(f"Could not load graph from {self.pkl_path}")
            
        self.schema_generator = SchemaGenerator(self.graph)
        
        # Initialize LLM Client
        # Check available keys and select provider
        if provider:
            logger.info(f"Using {provider} provider (model={model})")
            self.llm_client = LLMClient(provider=provider, model=model)
        elif os.getenv("GEMINI_API_KEY"):
            logger.info("Using Gemini provider")
            self.llm_client = LLMClient(provider="gemini", model="gemini-2.0-flash")
        elif os.getenv("OPENAI_API_KEY"):
            logger.info("Using OpenAI provider")
            self.llm_client = LLMClient(provider="openai", model="gpt-4o")
        else:
            logger.info("Using Ollama provider")
            self.llm_client = LLMClient(provider="ollama", model="deepseek-r1:7b")
        
        # Reload prompts to ensure our new YAML is loaded
        if hasattr(self.llm_client.prompter, 'reload'):
            self.llm_client.prompter.reload()

    def generate_brief_schema(self) -> str:
        """Generates brief schema description for all tables."""
        descriptions = []
        # Get all table names from the schema generator
        tables = self.schema_generator.tables.keys()
        
        for table_name in tables:
            desc = self.schema_generator.generate_combined_description(table_name, detail_level="brief")
            descriptions.append(desc)
        return "\n".join(descriptions)

    def generate_full_schema_for_selected(self, selected_columns: dict) -> str:
        """Generates full schema description for selected tables and columns."""
        # Wrap the result to match the expected format for generate_combined_description_for_selected
        # It expects a dictionary of candidate results, where each result has "selected_columns"
        # Since we have a single result, we wrap it.
        candidate_wrapper = {
            "final_result": {
                "selected_columns": selected_columns
            }
        }
        
        return self.schema_generator.generate_combined_description_for_selected(
            candidate_wrapper, 
            detail_level="full"
        )

    def schema_linking(self, question: str) -> dict:
        """
        Step 1: Schema Linking (Brief Schema -> Selected Columns)
        Returns: dict containing selected_columns, reasoning, prompts, and raw_response.
        """
        logger.info("Starting Schema Linking...")
        brief_schema = self.generate_brief_schema()
        
        # Prepare Prompt
        user_prompt = self.llm_client.prompter.get_prompt(
            "simple_sl_user", 
            db_name=self.db_name, 
            question=question, 
            brief_schema=brief_schema
        )
        system_prompt = self.llm_client.prompter.get_prompt("simple_sl_system")
        
        # Call LLM
        response = self.llm_client.ask(user_prompt, system=system_prompt)
        
        # Parse Text Output (Line-based)
        selected_columns = {}
        reasoning = ""
        
        try:
            # 1. Extract Reasoning
            block_pattern = r"```schema_links\s*(.*?)\s*```"
            match = re.search(block_pattern, response, re.DOTALL)
            
            if match:
                block_content = match.group(1).strip()
                reasoning = response.replace(match.group(0), "").strip()
            else:
                logger.warning("No ```schema_links``` block found. Attempting fuzzy parsing.")
                block_content = response.strip()
                reasoning = "Parsing failed to separate reasoning."

            # 2. Parse Lines
            lines = block_content.split('\n')
            for line in lines:
                line = line.strip()
                if not line or ':' not in line:
                    continue
                
                parts = line.split(':', 1)
                table_name = parts[0].strip()
                columns_str = parts[1].strip()
                
                if table_name not in self.schema_generator.tables:
                    logger.warning(f"Table '{table_name}' not found in schema. Skipping.")
                    continue
                
                if columns_str == "*":
                    all_cols = self.schema_generator.explorer.get_columns_for_table(table_name).keys()
                    cols_list = list(all_cols)
                else:
                    cols_list = [c.strip() for c in columns_str.split(',') if c.strip()]
                    valid_cols = []
                    table_cols = self.schema_generator.explorer.get_columns_for_table(table_name).keys()
                    for c in cols_list:
                        if c == "*": 
                             valid_cols.extend(list(table_cols))
                        elif c in table_cols:
                            valid_cols.append(c)
                        else:
                            logger.warning(f"Column '{c}' not found in table '{table_name}'. Skipping.")
                    cols_list = valid_cols
                
                if table_name in selected_columns:
                    selected_columns[table_name].extend(cols_list)
                    selected_columns[table_name] = list(set(selected_columns[table_name]))
                else:
                    selected_columns[table_name] = cols_list

            logger.info(f"Schema Linking selected columns: {selected_columns}")
            logger.info(f"Reasoning: {reasoning[:200]}...") 
            
            # Return enriched result
            return {
                "selected_columns": selected_columns,
                "reasoning": reasoning,
                "prompts": {"user": user_prompt, "system": system_prompt},
                "raw_response": response
            }
            
        except Exception as e:
            logger.error(f"Failed to parse Schema Linking response: {response}")
            raise ValueError("Invalid response format from Schema Linking step") from e

    def generate_sql(self, question: str, selected_columns: dict) -> dict:
        """
        Step 2: SQL Generation (Full Schema of Selected Subgraph -> SQL)
        Returns: dict containing sql, prompts, and raw_response.
        """
        logger.info("Starting SQL Generation...")
        
        if not selected_columns:
            logger.warning("No tables/columns selected from Schema Linking. Cannot generate SQL.")
            return {"sql": "-- No tables selected.", "prompts": {}, "raw_response": ""}
            
        full_schema = self.generate_full_schema_for_selected(selected_columns)
        
        # Prepare Prompt
        user_prompt = self.llm_client.prompter.get_prompt(
            "simple_sql_user", 
            db_name=self.db_name, 
            question=question, 
            full_schema=full_schema
        )
        system_prompt = self.llm_client.prompter.get_prompt("simple_sql_system")
        
        # Call LLM
        response = self.llm_client.ask(user_prompt, system=system_prompt)
        
        # Extract SQL from markdown
        sql_match = re.search(r"```sql\s*(.*?)\s*```", response, re.DOTALL)
        if sql_match:
            sql = sql_match.group(1).strip()
        else:
            code_match = re.search(r"```\s*(.*?)\s*```", response, re.DOTALL)
            if code_match:
                sql = code_match.group(1).strip()
            else:
                sql = response.strip()
                
        return {
            "sql": sql,
            "prompts": {"user": user_prompt, "system": system_prompt},
            "raw_response": response
        }

    def run(self, question: str):
        """Execute the full pipeline"""
        print(f"Question: {question}")
        
        # 1. Schema Linking
        sl_result = self.schema_linking(question)
        selected_columns = sl_result["selected_columns"]
        
        if not selected_columns:
            print("No tables selected. Aborting SQL generation.")
            return None
            
        # 2. SQL Generation
        sql_result = self.generate_sql(question, selected_columns)
        sql = sql_result["sql"]
        
        print("\nGenerated SQL:")
        print(sql)
        return sql

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Simple NL2SQL Pipeline")
    parser.add_argument("--db", type=str, required=True, help="Database name (e.g., california_schools)")
    parser.add_argument("--q", type=str, required=True, help="Natural language question")
    parser.add_argument("--dataset", type=str, default="bird", help="Dataset name (default: bird)")
    
    args = parser.parse_args()
    
    pipeline = SimpleSQLPipeline(args.db, args.dataset)
    pipeline.run(args.q)
