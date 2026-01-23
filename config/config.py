from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# LLM 配置
LLM_CONFIG = {
    "llm": {
        "current_env": "prd",
        "base_url": "http://lmsgw.lms.smb956101.com",
        "model": "Qwen3-32B",
        "authorization": "sk-t_xguppKyzVIUiEvAlkGxw"
    }
}