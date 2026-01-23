import re
from datetime import datetime
from decimal import Decimal
from typing import Optional, Any, Dict
import json
from utils.logger_util import get_logger

logger = get_logger()

current_date = datetime.now().strftime("%Y%m%d")

def parse_json_response(data: Any, default: Optional[str] = None) -> str:
    """解析并规范化 JSON 响应数据为 JSON 字符串
    Args:
        data (Any): 原始数据，可以是 dict、list 或其他类型
        default (Optional[str]): 默认返回值，若解析失败则返回此字符串，默认为 None
    Returns:
        str: 格式化的 JSON 字符串
    Raises:
        ValueError: 如果数据无法转换为有效的 JSON 字符串且未提供 default
    """
    try:
        if data is None:
            return "{}" if default is None else default
        return json.dumps(data, ensure_ascii=False, indent=2)
    except TypeError as e:
        logger.error(f"数据类型错误，无法转换为 JSON: {e}, 原始数据: {data}")
        if default is not None:
            return default
        raise ValueError(f"无法转换为有效 JSON 字符串: {e}")
    except Exception as e:
        logger.error(f"解析数据时发生错误: {e}, 原始数据: {data}")
        if default is not None:
            return default
        raise ValueError(f"数据处理失败: {e}")

def format_date(date_str: Optional[str]) -> Optional[str]:
    """
    将数据库中的日期格式(YYYYMMDD)或datetime对象转换为标准格式(YYYY-MM-DD)

    Args:
        date_str: 数据库中的日期字符串, 格式为YYYYMMDD, 或 datetime 对象

    Returns:
        格式化后的日期字符串, 格式为YYYY-MM-DD, 如果输入为空、无效或为默认日期(19000101)则返回None
    """
    # 如果是 datetime 对象
    if isinstance(date_str, datetime):
        return date_str.strftime("%Y-%m-%d") if date_str else None

    # 检查输入是否为空或长度不正确
    if not date_str or len(date_str) != 8 or not date_str.isdigit():
        return None

    # 检查是否为默认日期
    if date_str == "19000101":
        return None

    try:
        # 验证日期有效性并转换为目标格式
        parsed_date = datetime.strptime(date_str, "%Y%m%d")
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        return None

def decimal_to_float(data: Dict) -> Dict:
    """将字典中的Decimal类型转换为float"""
    for key, value in data.items():
        if isinstance(value, Decimal):
            data[key] = float(value)
        elif isinstance(value, dict):
            data[key] = decimal_to_float(value)
    return data

def parse_json_content(content):
    content = content.strip()
    if content.startswith('```json'):
        content = content[7:].lstrip('\n')
    if content.endswith('```'):
        content = content[:-3].rstrip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # Attempt to fix by truncating after the last expected key ("时间序列" for features)
        match = re.search(r'("时间序列"\s*:\s*\[[^\]]*\])\s*,?\s*', content, re.DOTALL)
        if match:
            end_pos = match.end()
            fixed_content = content[:end_pos].rstrip(',') + '\n}'
            try:
                return json.loads(fixed_content)
            except json.JSONDecodeError:
                pass
        # For simpler objects like {"id": ""}, just try to load again or handle
        raise ValueError(f"Invalid JSON: {content}")