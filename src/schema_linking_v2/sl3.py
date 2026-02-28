import json
import re
from typing import Dict, List
import logging

from src.llm.clients import LLMClient
from src.schema_linking_v2.candidate_filter import CandidateFilter
from src.utils.schema_generator import SchemaGenerator
from src.utils.graph_explorer import GraphExplorer
from src.llm.prompt_manager import PromptManager

logger = logging.getLogger(__name__)

class CandidateSelector:
    def __init__(self, dataset_name, db_name, explorer: GraphExplorer, question_data=None):
        self.dataset_name = dataset_name
        self.db_name = db_name
        self.question_data = question_data
        self.evidence = "### Evidence:\n" + self.question_data.get("evidence", "") if \
            self.question_data and self.question_data.get("evidence") else ""
        
        self.explorer = explorer
        self.sg = SchemaGenerator(self.explorer.graph)
        self.client = LLMClient("deepseek", "deepseek-chat")
        
        self.prompt_manager = PromptManager()
        self.prompt_manager.reload()

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

    def select_candidate(self, candidate_result_input, question: str, recommend_tables: str, keyword_hints,
                         result_from_last_round: str = '', detail_level="full") -> (dict, bool, dict):
        """
        Filter and select best candidate.
        Returns: (final_result, is_consistent, prompts_dict)
        """
        if isinstance(candidate_result_input, str):
            result_dict = json.loads(candidate_result_input)
        else:
            result_dict = candidate_result_input
            
        if result_from_last_round != '':
            result_from_last_round = "### Result From Last Round::\n" + result_from_last_round
            
        final_result, is_consistent = CandidateFilter.schema_linking_final_answer(result_dict)

        if is_consistent:
            logger.info("Candidate results consistent, returning final result directly.")
            return final_result, is_consistent, {}
        else:
            # Candidate results inconsistent, use LLM to judge
            candidate_schema = self.sg.generate_combined_description_for_selected(result_dict, detail_level=detail_level)
            
            user_prompt = self.prompt_manager.get_prompt(
                "sl2_candidate_user",
                candidate_results=json.dumps(final_result, indent=4, ensure_ascii=False)
            )
            
            # Note: system prompt needs variables too
            system_prompt_filled = self.prompt_manager.get_prompt(
                "sl2_candidate_system",
                db_name=self.db_name,
                question=question,
                recommend_tables=recommend_tables,
                evidence=self.evidence,
                keyword_hints=keyword_hints,
                candidate_schema=candidate_schema,
                result_from_last_round=result_from_last_round
            )
            
            messages = [
                {"role": "system", "content": system_prompt_filled},
                {"role": "user", "content": user_prompt}
            ]

            response = self.client.chat(messages)
            try:
                output = self.extract_json(response)
                # Return prompts separately
                return output, is_consistent, {"system": system_prompt_filled, "user": user_prompt}
            except Exception as e:
                try:
                    output = self._repair_json(response)
                    return output, is_consistent, {"system": system_prompt_filled, "user": user_prompt}
                except Exception as repair_e:
                    raise ValueError(f"SL3 JSON parse failed: {e}") from repair_e
