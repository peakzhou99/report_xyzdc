import time
from openai import OpenAI
import logging
import os

class LLMClient:
    def __init__(self, config):
        self.logger = logging.getLogger("LLMClient")
        self.config = config
        self.client = self._init_openai_client()

    def _init_openai_client(self):
        curr_env = os.getenv("CURRENT_ENV", "dev")
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


    def generate(self, prompts, temperature=0):
        # 提取文本信息
        try:
            response = self.client.chat.completions.create(
                model=self.config.get("model"),
                messages=prompts,
                temperature=temperature,
                response_format={"type": "json_object"},
                # timeout=30
            )
            # return response.choices[0].message.content
            return response.choices[0].message.reasoning_content
        except Exception as e:
            raise Exception(f"An error occurred while calling OpenAI API: {e}")
