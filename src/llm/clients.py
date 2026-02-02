import os
import requests
import json
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union, Optional

logger = logging.getLogger(__name__)


class BaseLLMClient(ABC):
    """
    LLM客户端的抽象基类。
    定义了与不同LLM提供者交互的标准接口。
    """

    @abstractmethod
    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        """
        根据提示生成文本。

Args：
            提示：用户输入提示。
            system_prompt：可选系统指令。
            **kwargs：额外模型参数（温度、max_tokens等）
Return:
            生成文本字符串。
        """
        pass

    @abstractmethod
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        Chat completion interface.
        
        Args:
            messages: List of message dicts [{'role': 'user', 'content': '...'}, ...]
            **kwargs: Additional model parameters.
            
        Returns:
            Response content string.
        """
        pass


class OpenAIClient(BaseLLMClient):
    """
    OpenAI兼容API客户端（OpenAI、DeepSeek、vLLM等）。
    需要安装'openai'包。
    """

    def __init__(self, api_key: str, base_url: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("Please install openai package: pip install openai")

        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        return self.chat(messages, **kwargs)

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        try:
            # Merge default params with kwargs
            params = {
                "model": self.model,
                "messages": messages,
                "temperature": 0.0,
            }
            params.update(kwargs)

            response = self.client.chat.completions.create(**params)
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API Call Error: {e}")
            raise


class OllamaClient(BaseLLMClient):
    """
    Ollama私有部署客户端。
    直接与Ollama API端点交互。
    """

    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3"):
        self.base_url = base_url.rstrip('/')
        self.model = model

    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        url = f"{self.base_url}/api/generate"

        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": kwargs
        }

        if system_prompt:
            payload["system"] = system_prompt

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json().get("response", "")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ollama API Error: {e}")
            raise

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        url = f"{self.base_url}/api/chat"

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "options": kwargs
        }

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json().get("message", {}).get("content", "")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ollama Chat API Error: {e}")
            raise


from google import genai
from google.genai import types


class GeminiClient(BaseLLMClient):
    """
    使用最新的 Google Gen AI SDK (支持 Gemini 2.0)
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        # 新版不再使用全局 configure，而是实例化 Client 对象
        self.client = genai.Client(api_key=api_key)
        self.model_id = model

    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        try:
            # 系统指令直接放在 config 中
            config = {}
            if system_prompt:
                config["system_instruction"] = system_prompt

            # 合并其他参数（如 temperature, max_output_tokens 等）
            config.update(kwargs)

            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=config
            )
            return response.text
        except Exception as e:
            logger.error(f"Gemini Generate Error: {e}")
            raise

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        try:
            # 1. 提取系统指令
            system_instruction = None
            history = []

            # 2. 转换历史消息格式
            for msg in messages[:-1]:
                if msg['role'] == 'system':
                    system_instruction = msg['content']
                else:
                    # 注意：Gemini 角色必须是 'user' 或 'model'
                    role = "model" if msg['role'] == 'assistant' else "user"
                    history.append({"role": role, "parts": [{"text": msg['content']}]})

            # 3. 获取最后一条消息作为当前输入
            last_msg = messages[-1]['content']

            # 4. 创建会话并发送
            chat = self.client.chats.create(
                model=self.model_id,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    **kwargs
                ),
                history=history
            )

            response = chat.send_message(last_msg)
            return response.text
        except Exception as e:
            logger.error(f"Gemini Chat Error: {e}")
            raise


class LLMFactory:
    """
    LLM客户端工厂类。
    根据配置创建不同LLM客户端。
    """

    @staticmethod
    def create_client(provider: str, **config) -> BaseLLMClient:
        """
        Create an LLM client.
        
        Args:
            provider: 'openai', 'ollama', or 'gemini'
            **config: Configuration arguments for the specific client.
        """
        provider = provider.lower()

        if provider == "openai":
            return OpenAIClient(
                api_key=config.get("api_key", os.getenv("OPENAI_API_KEY")),
                base_url=config.get("base_url"),
                model=config.get("model", "gpt-3.5-turbo")
            )
        elif provider == "ollama":
            return OllamaClient(
                base_url=config.get("base_url", "http://localhost:11434"),
                model=config.get("model", "llama3")
            )
        elif provider == "gemini":
            return GeminiClient(
                api_key=config.get("api_key", os.getenv("GEMINI_API_KEY")),
                model=config.get("model", "gemini-pro")
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")


# 示例代码
if __name__ == '__main__':
    print("--- 1. OpenAI Client Example ---")
    # openai_client = LLMFactory.create_client(
    #     "openai",
    #     api_key="your-api-key",
    #     base_url="https://api.deepseek.cn/v1",
    #     model="deepseek-3.5"
    # )

    print("--- 2. Ollama Client Example ---")
    # ollama_client = LLMFactory.create_client(
    #     "ollama",
    #     base_url="http://localhost:11434",
    #     model="llama3"
    # )

    print("--- 3. Gemini Client Example ---")
    try:
        # 使用 provider="gemini" 调用原生 Google SDK
        my_gemini = LLMFactory.create_client(
            "gemini",
            # 请替换为有效的 Google AI Studio API Key
            api_key="AIzaSyA442ZAvdXJ5aI0zFfPaNlglZnmaiV5r3E",
            model="gemini-2.0-flash"
        )
        print("Gemini client initialized.")
        # 测试生成 (如果有有效key可取消注释)
        print(my_gemini.generate("Hello, Gemini!"))
    except Exception as e:
        print(f"Gemini example failed: {e}")
