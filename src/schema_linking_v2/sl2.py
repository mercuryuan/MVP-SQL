from typing import List, Dict
import json
import re
import logging

from src.llm.clients import LLMClient
from src.utils.schema_generator import SchemaGenerator
from src.utils.graph_explorer import GraphExplorer
from src.llm.prompt_manager import PromptManager

logger = logging.getLogger(__name__)

class SubgraphSelector:
    def __init__(self, dataset_name, db_name, explorer: GraphExplorer, question_data=None):
        self.dataset_name = dataset_name
        self.db_name = db_name
        self.question_data = question_data
        self.evidence = "### Evidence:\n" + self.question_data.get("evidence", "") if \
            self.question_data and self.question_data.get("evidence") else ""
        
        self.explorer = explorer
        self.schema_generator = SchemaGenerator(self.explorer.graph)
        
        # Use deepseek if available
        self.client = LLMClient("deepseek", "deepseek-chat")
        
        self.prompt_manager = PromptManager()
        self.prompt_manager.reload()
        
        # Preload system prompt
        self.system_prompt = self.prompt_manager.get_prompt("sl2_expansion_system")
        
        self.sl2_per_iterations = []

    def extract_json(self, text: str) -> Dict:
        """Extract JSON content from the given text."""
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        try:
            return json.loads(text)
        except Exception:
            matches = re.findall(r'\{.*\}', text, re.DOTALL)
            if matches:
                try:
                    return json.loads(matches[0])
                except:
                    pass
                try:
                    return json.loads(matches[-1])
                except:
                    pass
            raise ValueError("No valid JSON found in response")

    def _repair_client(self) -> LLMClient:
        if getattr(self.client, "provider", None) == "deepseek" and getattr(self.client, "model", None) == "deepseek-reasoner":
            return LLMClient(provider="deepseek", model="deepseek-chat")
        return self.client

    def _repair_json(self, raw_response: str) -> Dict:
        system = "You are a strict JSON formatter. Output only RAW JSON. No markdown. No explanations."
        schema = (
            "{"
            "\"selected_columns\": {\"table\": [\"col\"]},"
            "\"selected_reference_path\": {\"t1.c=t2.c\": \"why\"},"
            "\"reasoning\": {\"table\": \"why\"},"
            "\"to_solve_the_question\": {\"is_solvable\": true, \"question\": \"\", \"reason\": \"\"}"
            "}"
        )
        user = (
            "Convert the following content into a JSON object that strictly matches this schema:\n"
            f"{schema}\n\n"
            "If the content lacks information, return empty objects/lists but keep required keys.\n\n"
            "Content:\n"
            f"{raw_response}\n\n"
            "Return RAW JSON only."
        )
        repaired = self._repair_client().chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}]
        )
        return self.extract_json(repaired)

    def select_relevant_tables(self, db_schema: str, question: str, select_table: List[str], keyword_hints='',
                               result_from_last_round='', hint='') -> (Dict, Dict):
        
        user_content = self.prompt_manager.get_prompt(
            "sl2_expansion_user",
            db_name=self.db_name,
            db_schema=db_schema,
            question=question,
            keyword_hints=keyword_hints,
            evidence=self.evidence,
            select_table=select_table,
            result_from_last_round=result_from_last_round,
            hint=hint
        )

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content}
        ]

        # Call API
        raw_response = self.client.chat(messages)

        if not raw_response:
            raise ValueError("SL2 empty response from LLM")

        try:
            result = self.extract_json(raw_response)
            return result, {"system": self.system_prompt, "user": user_content}
        except Exception as e:
            try:
                result = self._repair_json(raw_response)
                return result, {"system": self.system_prompt, "user": user_content}
            except Exception as repair_e:
                raise ValueError(f"SL2 JSON parse failed: {e}") from repair_e

    def generate_schema_description(self, selected_table: List[str]=None, detail_level="brief"):
        """
        Get multi-hop subgraph from explorer.
        selected_table is a list containing currently selected tables.
        In schema it is 0-hop, based on this show 1-hop tables and columns.
        """
        schema = []
        if selected_table:
            # bfs_subgraph returns list of layers (lists of table names)
            n_hop_list = self.explorer.bfs_subgraph(selected_table)
            for i, hop in enumerate(n_hop_list):
                if i <= 1:
                    schema.append(f"-----------------{i} hop-------------------")
                for t in hop:
                    if i == 0:
                        schema.append(self.schema_generator.generate_combined_description(t, detail_level))
                    elif i == 1:
                        # 1-hop also follows detail_level? Or always brief? 
                        # Original code used brief for 1-hop. But let's use the param.
                        schema.append(self.schema_generator.generate_combined_description(t, detail_level, selected_table))
            return "\n".join(schema)
        else:
            # No selected_tables provided, return all tables
            tables = list(self.explorer.get_all_tables().keys())
            for table in tables:
                schema.append(self.schema_generator.generate_combined_description(table, detail_level))
            return "\n".join(schema)

    def generate_result_from_last_round(self, result: str):
        return "### Result from last round:\n" + result

    def generate_hint(self, hint: str):
        return "### Recommendation table(s):\n" + hint
