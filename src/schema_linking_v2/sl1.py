from typing import List, Dict
import json
import re
import logging

from src.llm.clients import LLMClient
from src.utils.schema_generator import SchemaGenerator
from src.utils.graph_explorer import GraphExplorer
from src.llm.prompt_manager import PromptManager

logger = logging.getLogger(__name__)

class TableSelector:
    def __init__(self, dataset_name, db_name, explorer: GraphExplorer, question_data=None):
        self.dataset_name = dataset_name
        self.db_name = db_name
        self.explorer = explorer
        # Use src.utils.schema_generator.SchemaGenerator which takes a graph
        self.schema_generator = SchemaGenerator(self.explorer.graph)
        self.question_data = question_data
        self.evidence = "### Evidence:\n" + self.question_data.get("evidence", "") if \
            self.question_data and self.question_data.get("evidence") else ""
        
        self.client = LLMClient(provider="deepseek", model="deepseek-chat")
        
        self.prompt_manager = PromptManager()
        self.prompt_manager.reload()
        
        # Preload system prompt
        self.system_prompt = self.prompt_manager.get_prompt("sl2_initial_selection_system")

    def extract_json(self, text: str) -> Dict:
        """Improved JSON extraction with error handling"""
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            text = text.strip()
            text = re.sub(r"(?<!\\)\\", "", text)
            
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

            raise ValueError(f"No valid JSON found in: {text[:200]}...")

    def _repair_client(self) -> LLMClient:
        if getattr(self.client, "provider", None) == "deepseek" and getattr(self.client, "model", None) == "deepseek-reasoner":
            return LLMClient(provider="deepseek", model="deepseek-chat")
        return self.client

    def _repair_json(self, raw_response: str, candidate_tables: List[str]) -> Dict:
        system = "You are a strict JSON formatter. Output only RAW JSON. No markdown. No explanations."
        schema = (
            "{"
            "\"selected_entity\": [\"table1\", \"table2\"],"
            "\"reasoning\": {\"<selected_table>\": \"<why selected>\"},"
            "\"the steps of decomposed the question\": [\"step1\"],"
            "\"value_keywords\": [\"keyword1\"]"
            "}"
        )
        table_hint = ", ".join(candidate_tables[:50])
        user = (
            "Convert the following content into a JSON object that strictly matches this schema:\n"
            f"{schema}\n\n"
            "Rules:\n"
            "- Use only table names that appear in the content.\n"
            "- If unsure, return an empty selected_entity list.\n\n"
            f"Known tables mentioned in content (optional hint): {table_hint}\n\n"
            "Content:\n"
            f"{raw_response}\n\n"
            "Return RAW JSON only."
        )
        repaired = self._repair_client().chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}]
        )
        return self.extract_json(repaired)

    def select_relevant_tables(self, db_schema: str, question: str) -> (Dict, Dict):
        # Get user prompt from YAML
        user_content = self.prompt_manager.get_prompt(
            "sl2_initial_selection_user",
            db_name=self.db_name,
            db_schema=db_schema,
            question=question,
            evidence=self.evidence
        )
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content}
        ]

        raw_response = self.client.chat(messages)
        try:
            result = self.extract_json(raw_response)
            self._validate_result_structure(result)
            return result, {"system": self.system_prompt, "user": user_content}
        except Exception as e:
            candidate_tables = []
            for t in self.explorer.get_all_tables().keys():
                if re.search(rf"\b{re.escape(str(t))}\b", raw_response, re.IGNORECASE):
                    candidate_tables.append(t)
            try:
                result = self._repair_json(raw_response, candidate_tables)
                self._validate_result_structure(result)
                return result, {"system": self.system_prompt, "user": user_content}
            except Exception as repair_e:
                heuristic = candidate_tables[:5]
                if heuristic:
                    return {
                        "selected_entity": heuristic,
                        "reasoning": {},
                        "the steps of decomposed the question": [],
                        "value_keywords": []
                    }, {"system": self.system_prompt, "user": user_content}
                raise ValueError(f"SL1 JSON parse failed: {e}") from repair_e

    def _validate_result_structure(self, result: Dict):
        """Validate result structure"""
        required_keys = [
            "selected_entity",
            "reasoning",
            "the steps of decomposed the question"
        ]
        for key in required_keys:
            if key not in result:
                raise ValueError(f"Missing required key: {key}")

        if not isinstance(result["selected_entity"], list):
            raise TypeError("selected_entity must be a list")

        if len(result["selected_entity"]) > 5:
            raise ValueError("Maximum 5 tables allowed")
