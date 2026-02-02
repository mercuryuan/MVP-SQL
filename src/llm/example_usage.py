import os
import sys

# 将项目根目录添加到 python 路径，确保能导入 src 模块
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from src.llm import LLMFactory, PromptManager


def main():
    print("=== LLM 模块使用极简示例 ===\n")

    # ---------------------------------------------------------
    # 第一步：管理 Prompt (提示词)
    # ---------------------------------------------------------
    print("1. 正在加载 Prompt 管理器...")

    # 初始化管理器，它会自动读取 configs/prompts/ 下的所有 yaml 文件
    pm = PromptManager()

    # 演示：从 default.yaml 中读取并填充提示词
    # 假设 yaml 里有个 key 叫 'sql_generation_user'，内容包含 {schema} 和 {question} 占位符
    try:
        system_prompt = pm.get_prompt("sql_generation_system")
        user_prompt = pm.get_prompt(
            "sql_generation_user",
            schema="表名: 用户表(id, 姓名)",
            question="查询所有用户"
        )

        print(f"-> 加载到的系统提示词: {system_prompt}")
        print(f"-> 填充后的用户提示词: {user_prompt}\n")
    except KeyError as e:
        print(f"错误: 找不到对应的 Prompt Key: {e}")
        return

    # ---------------------------------------------------------
    # 第二步：调用 OpenAI 接口 (闭源模型示例)
    # ---------------------------------------------------------
    print("2. 初始化 OpenAI 客户端...")

    # 使用工厂模式创建客户端，API Key 这里只是占位符
    openai_client = LLMFactory.create_client(
        provider="openai",
        api_key="sk-your-api-key-here",  # 这里填入你的真实 Key
        model="gpt-3.5-turbo"
    )

    # 模拟调用 (实际调用只需取消注释)
    # response = openai_client.generate(prompt=user_prompt, system_prompt=system_prompt)
    print("-> OpenAI 客户端创建成功，准备好调用 generate() 方法了。")

    # ---------------------------------------------------------
    # 第三步：调用 Ollama 接口 (私有化部署示例)
    # ---------------------------------------------------------
    print("\n3. 初始化 Ollama 客户端...")

    ollama_client = LLMFactory.create_client(
        provider="ollama",
        base_url="http://localhost:11434",
        model="llama3"
    )

    # 模拟调用
    # response = ollama_client.generate(prompt=user_prompt, system_prompt=system_prompt)
    print("-> Ollama 客户端创建成功，准备好调用 generate() 方法了。")

    # ---------------------------------------------------------
    # 第四步：调用 Gemini 接口 (Google AI 示例)
    # ---------------------------------------------------------
    print("\n4. 初始化 Gemini 客户端...")

    try:
        gemini_client = LLMFactory.create_client(
            provider="gemini",
            api_key="AIzaSyA442ZAvdXJ5aI0zFfPaNlglZnmaiV5r3E",  # 这里填入你的真实 Key
            model="gemini-pro"
        )
        # 模拟调用
        response = gemini_client.generate(prompt=user_prompt, system_prompt=system_prompt)
        print("-> Gemini 客户端创建成功，准备好调用 generate() 方法了。")
    except Exception as e:
        print(f"-> Gemini 初始化失败: {e}")


if __name__ == "__main__":
    main()
