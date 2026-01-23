
from openai import OpenAI
import logging
from typing import Generator, Union
from pathlib import Path
import sys
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
from config.config import LLM_CONFIG

class LLMClient:
    def __init__(self, config):
        self.logger = logging.getLogger("LLMClient")
        self.config = config
        self.client = self._init_openai_client()

    def _init_openai_client(self):
        curr_env = self.config.get("current_env", "dev")
        openai_api_base = self.config.get("base_url")
        openai_header_auth = self.config.get("authorization")
        openai_api_key = "EMPTY"

        if openai_header_auth is None:
            raise ValueError(f"API key for {curr_env} environment is missing.")
        client = OpenAI(
            api_key=openai_api_key,
            base_url=openai_api_base,
            default_headers={
                "Authorization": f"Bearer {openai_header_auth}"
            }
        )
        self.logger.info("LLM客户端创建成功")
        return client

    def build_prompt(self, text: str, messages: list) -> list:
        local_messages = messages.copy()
        local_messages.append({
            "role": "user",
            "content": f"{text}"
        })
        return local_messages

    def generate_stream(self, prompts, temperature=0) -> Generator[str, None, None]:
        """
        流式生成响应
        """
        try:
            stream = self.client.chat.completions.create(
                model=self.config.get("model"),
                messages=prompts,
                temperature=temperature,
                response_format={"type": "json_object"},
                stream=True  # 开启流式传输
            )

            for chunk in stream:
                # 提取流式响应中的内容
                if chunk.choices[0].delta.content is not None:
                    yield chunk.choices[0].delta.content

        except Exception as e:
            raise Exception(f"An error occurred while calling OpenAI API (streaming): {e}")

    # def generate(self, prompts, temperature=0) -> Union[str, Generator[str, None, None]]:
    #     """
    #     非流式
    #     """
    #     try:
    #         response = self.client.chat.completions.create(
    #             model=self.config.get("model"),
    #             messages=prompts,
    #             temperature=temperature,
    #             response_format={"type": "json_object"},
    #         )
    #         return response.choices[0].message.reasoning_content
    #     except Exception as e:
    #         raise Exception(f"An error occurred while calling OpenAI API: {e}")

    def generate_stream_complete(self, prompts, temperature=0) -> str:
        """
        使用流式API但返回完整内容
        """
        try:
            stream = self.client.chat.completions.create(
                model=self.config.get("model"),
                messages=prompts,
                temperature=temperature,
                response_format={"type": "json_object"},
                stream=True
            )

            complete_content = ""
            for chunk in stream:
                if chunk.choices[0].delta.content is not None:
                    complete_content += chunk.choices[0].delta.content

            return complete_content

        except Exception as e:
            raise Exception(f"An error occurred while calling OpenAI API (streaming complete): {e}")


# 使用示例
if __name__ == "__main__":
    # 配置示例
    client = LLMClient(LLM_CONFIG['llm'])
    messages = [{"role": "system", "content": "You are a helpful assistant."}]
    prompts = client.build_prompt("Hello, how are you?", messages)

    # 方式1: 流式生成，实时获取响应片段
    print("=== 流式响应 ===")
    for chunk in client.generate_stream(prompts):
        print(chunk, end='', flush=True)
    print("\n")

    # # 方式2: 使用统一接口，通过参数控制
    # print("=== 统一接口流式 ===")
    # for chunk in client.generate(prompts, stream=True):
    #     print(chunk, end='', flush=True)
    # print("\n")

    # 方式3: 流式API但获取完整内容
    print("=== 流式API完整内容 ===")
    complete_response = client.generate_stream_complete(prompts)
    print(complete_response)