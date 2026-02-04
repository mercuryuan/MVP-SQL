import os
import requests
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv


load_dotenv()

# 调整日志级别，屏蔽烦人的 httpx info
logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# --- 1. 核心驱动层 (保持不变) ---
class BaseDriver(ABC):
    @abstractmethod
    def request(self, messages: List[Dict], **kwargs) -> str: pass


class OpenAIDriver(BaseDriver):
    def __init__(self, api_key: str, base_url: Optional[str] = None, model: str = "gpt-4o"):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def request(self, messages: List[Dict], **kwargs) -> str:
        params = {"model": self.model, "messages": messages, "temperature": 0, **kwargs}
        response = self.client.chat.completions.create(**params)
        return response.choices[0].message.content


class OllamaDriver(BaseDriver):
    def __init__(self, base_url: str, model: str):
        self.base_url = base_url.rstrip('/')
        self.model = model

    def request(self, messages: List[Dict], **kwargs) -> str:
        url = f"{self.base_url}/api/chat"
        payload = {"model": self.model, "messages": messages, "stream": False, "options": kwargs}
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json().get("message", {}).get("content", "")
        except Exception as e:
            logger.error(f"Ollama Error: {e}")
            raise


class GeminiDriver(BaseDriver):
    def __init__(self, api_key: str, model: str):
        from google import genai
        self.client = genai.Client(api_key=api_key)
        self.model = model

    def request(self, messages: List[Dict], **kwargs) -> str:
        sys_instr = next((m['content'] for m in messages if m['role'] == 'system'), None)
        last_msg = messages[-1]['content']
        response = self.client.models.generate_content(
            model=self.model, contents=last_msg, config={"system_instruction": sys_instr, **kwargs}
        )
        return response.text


# --- 2. 统一调用入口 (集成了 PromptManager) ---

class LLMClient:
    def __init__(self, provider: str = "openai", model: Optional[str] = None, prompt_dir: str = None, **kwargs):
        """
        :param prompt_dir: 指定提示词 YAML 文件的目录，如果不传则使用 PromptManager 的默认路径
        """
        self.provider = provider.lower()

        # 1. 初始化驱动 (Driver)
        if self.provider == "deepseek":
            self.driver = OpenAIDriver(
                api_key=kwargs.get("api_key", os.getenv("DEEPSEEK_API_KEY")),
                base_url="https://api.deepseek.com",
                # 模型只有deepseek-chat和deepseek-reasoner
                model=model or "deepseek-chat"
            )
        elif self.provider == "gemini":
            self.driver = GeminiDriver(
                api_key=kwargs.get("api_key", os.getenv("GEMINI_API_KEY")),
                model=model or "gemini-2.0-flash"
            )
        elif self.provider == "ollama":
            self.driver = OllamaDriver(
                base_url=kwargs.get("base_url", os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")),
                model=model or "llama3"
            )
        elif self.provider == "openai":
            self.driver = OpenAIDriver(
                api_key=kwargs.get("api_key", os.getenv("OPENAI_API_KEY")),
                model=model or "gpt-4o"
            )
        else:
            raise ValueError(f"Unknown provider: {self.provider}")

        try:
            from .prompt_manager import PromptManager
        except ImportError:
            # 如果相对导入失败（比如直接运行脚本时），尝试绝对导入
            from src.llm.prompt_manager import PromptManager

        # 2. 初始化 PromptManager
        # 这样 LLMClient 就拥有了管理提示词的能力
        self.prompter = PromptManager(prompt_dir=prompt_dir)

    def ask(self, prompt: str, system: str = "You are a helpful assistant", **kwargs) -> str:
        """原生接口：直接传字符串"""
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt}
        ]
        return self.driver.request(messages, **kwargs)

    def ask_with_template(self, template_name: str, variables: Dict[str, Any] = {}, system_template: str = None,
                          **kwargs) -> str:
        """
        高级接口：使用 YAML 中的模板

        :param template_name: YAML 文件中定义的 prompt key (如 'text_to_sql_v1')
        :param variables: 用于填充模板的变量 (如 {'schema': '...', 'question': '...'})
        :param system_template: 可选，YAML 中定义的系统提示词 key
        """
        # 1. 获取并填充用户提示词
        user_prompt = self.prompter.get_prompt(template_name, **variables)

        # 2. 获取系统提示词 (如果指定了 key，就去取；否则用默认的)
        if system_template:
            system_prompt = self.prompter.get_prompt(system_template)  # 假设系统提示词不需要动态填充
        else:
            system_prompt = "You are a helpful assistant."

        # 3. 发送请求
        return self.ask(user_prompt, system=system_prompt, **kwargs)


# --- 3. 使用示例 ---
if __name__ == '__main__':
    # 假设你有一个 sql.yaml 文件，内容如下：
    # system_sql: "你是一个数据库专家，只输出 SQL。"
    # generate_sql: "基于以下表结构：\n{schema}\n\n请生成查询：{question}"

    client = LLMClient(provider="gemini")
    # client = LLMClient(provider="deepseek")
    print(client.ask("你是谁？"))

    # 模拟数据
    schema_str = "Table: users (id, name, age)"
    user_q = "查询所有年龄大于20的用户"

    try:
        # 使用模板调用，代码非常干净
        sql = client.ask_with_template(
            template_name="sql_generation_user",
            variables={"schema": schema_str, "question": user_q},
            system_template="sql_generation_system"
        )
        print(sql)
    except KeyError as e:
        print(f"提示词未找到，请检查 YAML 文件: {e}")