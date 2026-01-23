from openai import OpenAI
from config.llm_config import CURR_ENV, ENV_CONFIG
from utils.log_utils import get_logger

logger = get_logger()


def qwen_client():
    # openai_api_base = "http://10.0.251.202:8888/v1"
    openai_api_key = "EMPTY"
    openai_api_base = ENV_CONFIG.get(CURR_ENV).get("base_url")
    openai_header_auth = ENV_CONFIG.get(CURR_ENV).get("authorization")
    return OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
        default_headers={
            "Authorization": f"Bearer {openai_header_auth}"
        }
    )



