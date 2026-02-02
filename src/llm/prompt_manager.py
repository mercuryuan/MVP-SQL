import os
import yaml
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class PromptManager:
    """
    Manages loading and formatting of prompts from external configuration files.
    Prompts are decoupled from code and stored in YAML files.
    """

    def __init__(self, prompt_dir: str = None):
        """
        Initialize PromptManager.
        
        Args:
            prompt_dir: Directory containing prompt YAML files. 
                        If None, defaults to 'configs/prompts' relative to project root.
        """
        if prompt_dir:
            self.prompt_dir = prompt_dir
        else:
            # Default to configs/prompts relative to project root
            # Assuming this file is in src/llm/prompt_manager.py
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            self.prompt_dir = os.path.join(project_root, "configs", "prompts")

        self.prompts: Dict[str, Any] = {}
        self._load_prompts()

    def _load_prompts(self):
        """Load all .yaml files from the prompt directory."""
        if not os.path.exists(self.prompt_dir):
            logger.warning(f"Prompt directory not found: {self.prompt_dir}")
            return

        for filename in os.listdir(self.prompt_dir):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                file_path = os.path.join(self.prompt_dir, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f)
                        if data:
                            self.prompts.update(data)
                            logger.info(f"Loaded prompts from {filename}")
                except Exception as e:
                    logger.error(f"Failed to load prompt file {filename}: {e}")

    def get_prompt(self, prompt_name: str, **kwargs) -> str:
        """
        Retrieve and format a prompt by name.
        
        Args:
            prompt_name: The key of the prompt in the YAML file.
            **kwargs: Variables to format into the prompt string.
            
        Returns:
            Formatted prompt string.
            
        Raises:
            KeyError: If prompt_name is not found.
        """
        if prompt_name not in self.prompts:
            raise KeyError(f"Prompt '{prompt_name}' not found in loaded prompts.")

        prompt_template = self.prompts[prompt_name]

        # If the prompt is a dictionary (e.g. system/user messages), handle differently if needed
        # For now, assuming simple string templates or handling simple string formatting
        if isinstance(prompt_template, str):
            try:
                return prompt_template.format(**kwargs)
            except KeyError as e:
                logger.warning(f"Missing key for prompt format: {e}")
                return prompt_template  # Return raw if formatting fails? Or raise?

        return prompt_template

    def reload(self):
        """Reload all prompts from disk."""
        self.prompts = {}
        self._load_prompts()
