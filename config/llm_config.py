CURR_ENV = 'PRD'
ENV_CONFIG = {
    "DEV": {
        "base_url": "http://10.0.251.202:8888/v1",
        "model": "Qwen2.5-72B-Instruct",
        "authorization":"sk-8WEss2tCGbLljIa106pJBg"
    },
    "TEST": {
        "base_url": "http://10.0.251.201:4000",
        "model": "Qwen2.5-72B-Instruct",
        "authorization": "sk-8WEss2tCGbLljIa106pJBg"
    },
    "PRD": {
        "base_url": "http://lmsgw.lms.smb956101.com",
        "model": "Qwen2.5-72B-Instruct",
        "authorization": "sk-swDKDq4RxF3bHM2DvXKfqg"
        # "base_url": "http://lmsgw.lms.smb956101.com",
        # "model": "Qwen3-32B",
        # "authorization": "sk-t_xguppKyzVIUiEvAlkGxw",
    },
    "LPRD": {
        "base_url": "http://lmsgw.lms.smb956101.com",
        "model": "qwen2-72B-Instruct",
        "authorization": "sk-P3fsW7VbGxAy9wVx-FK95A"
    },
}