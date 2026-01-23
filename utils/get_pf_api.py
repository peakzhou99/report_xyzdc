import requests
import json
from datetime import datetime, timedelta
import re


class CreditApprovalFetcher:
    def __init__(self):
        self.API_URL = "http://10.10.35.107:9080/js_dh_report"
        self.API_KEY = "785f2725e63243f5985df3e9dd0e6360"

    def _parse_approval_key(self, approval_no: str):
        """
        解析 approval_no，返回用于排序的 tuple: (year, month_day, seq_index)
        越大越新。
        示例：
            "苏商银信审【2025】第030501号" -> (2025, 305, 1)
            "苏商银信审[2025]第03051号"   -> (2025, 305, 1)
            "苏商银信审【2024】第12302号" -> (2024, 1230, 2)
        如果解析失败返回 (-1, -1, -1)
        """
        if not approval_no:
            return (-1, -1, -1)

        # 匹配年份：支持中文【】和英文[]
        year_match = re.search(r'[【\[](\d{4})[】\]]', approval_no)
        if not year_match:
            return (-1, -1, -1)
        year = int(year_match.group(1))

        # 匹配序号部分：第030501号 或 第03051 或 第030501（无号字）
        seq_match = re.search(r'第(\d+)[号]?', approval_no)
        if not seq_match:
            return (year, -1, -1)

        seq_str = seq_match.group(1)  # "030501" 或 "03051"

        # 至少要有4位（MMDD）
        if len(seq_str) < 4:
            return (year, -1, -1)

        try:
            month_day = int(seq_str[:4])      # 前4位：0305 -> 305
            seq_part = seq_str[4:]            # 后缀序号，可能为空
            seq_index = int(seq_part) if seq_part else 0
            return (year, month_day, seq_index)
        except ValueError:
            return (year, -1, -1)

    def _select_latest_approval(self, datas):
        """
        从 datas 列表中选出最新的一个批复
        规则：按 _parse_approval_key 解析后的 tuple 降序排序，取第一个
        如果没有有效的 approval_no，则按 reply_date 字符串倒序（兜底）
        """
        if not datas:
            return []

        if len(datas) == 1:
            return datas

        # 首先尝试用 approval_no 排序
        decorated = []
        for data in datas:
            key = self._parse_approval_key(data.get('approval_no', ''))
            decorated.append((key, data))

        # 按 key 降序（年份越大越前，月日越大越前，序号越大越前）
        decorated.sort(key=lambda x: x[0], reverse=True)

        # 检查第一个是否解析成功
        if decorated[0][0] != (-1, -1, -1):
            latest_data = decorated[0][1]
            return [latest_data]

        # 兜底：如果所有 approval_no 都解析失败，则按 reply_date 字符串倒序
        valid_datas = [d for d in datas if d.get('reply_date')]
        if not valid_datas:
            return [datas[0]]  # 随便返回一个

        valid_datas.sort(key=lambda x: x.get('reply_date', ''), reverse=True)
        return [valid_datas[0]]


    def get_credit_approval(self, cust_name, time_range: str = None):
        """
        真实 API 调用
        注意：time_range 参数现已被忽略，永远返回最新的一个批复
        """
        url = f"{self.API_URL}/getCreditApproval"
        headers = {
            "apikey": self.API_KEY,
            "Content-Type": "application/json;charset=UTF-8",
        }

        yesterday = datetime.now() - timedelta(days=1)
        data_date = yesterday.strftime('%Y%m%d')


        payload = {
            "params": {
                "data_date": data_date,
                "cust_name": f"{cust_name}"
            }
        }

        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)

            if response.status_code != 200:
                return {"error": "Request failed", "code": response.status_code, "details": response.text[:200]}

            content = response.text
            try:
                api_result = json.loads(content)
                if isinstance(api_result.get('data'), str):
                    api_result['data'] = json.loads(api_result['data'])
            except json.JSONDecodeError as e:
                return {"error": "Invalid JSON response", "details": str(e)}

            # 核心：选取最新的一个批复
            if isinstance(api_result.get('data'), dict):
                datas = api_result['data'].get('datas', [])
                latest_datas = self._select_latest_approval(datas)
                api_result['data']['datas'] = latest_datas

            return api_result

        except requests.exceptions.Timeout:
            return {"error": "Request timeout"}
        except requests.exceptions.RequestException as e:
            return {"error": "Request exception", "details": str(e)}
        except Exception as e:
            return {"error": "Unknown exception", "details": str(e)}


if __name__ == "__main__":
    fetcher = CreditApprovalFetcher()
    target_company = "都江堰兴市集团有限责任公司"

    print("\n=== 测试真实 API ===")
    result_real = fetcher.get_credit_approval(target_company)
    print(json.dumps(result_real, ensure_ascii=False, indent=2))