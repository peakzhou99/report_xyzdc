# 修改后的 get_regifinaplat_service.py (新增方法在文件末尾添加)

#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Description：查询地方融资平台信息

from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from sqlmodel import Session, select
from typing import List, Optional
from structured.models.tq_cibd_regifinaplat_model import TQ_CIBD_REGIFINAPLAT
from utils.db_util import engine
from utils.logger_util import get_logger

logger = get_logger()

def get_regifinaplat_by_itcode(itcode: str, session: Session) -> Optional[TQ_CIBD_REGIFINAPLAT]:
    """
    根据公司代码查询地方融资平台信息
    """
    try:
        logger.info(f"查询公司代码 '{itcode}' 的地方融资平台信息")
        result = session.exec(
            select(TQ_CIBD_REGIFINAPLAT)
            .where(TQ_CIBD_REGIFINAPLAT.ITCODE == itcode)
            .distinct()
        ).first()
        if not result:
            logger.error(f"未找到公司代码 '{itcode}' 的融资平台信息")
            return None
        logger.info(f"查询到融资平台信息: {result.ITNAME}")
        return result
    except Exception as e:
        logger.error(f"查询地方融资平台信息时出错: {e}")
        return None

def get_regifinaplat_by_finaffcode(finaffcode: str, session: Session) -> List[TQ_CIBD_REGIFINAPLAT]:
    """
    根据融资归属地代码查询地方融资平台信息
    """
    try:
        logger.info(f"查询融资归属地代码 '{finaffcode}' 的地方融资平台信息")
        results = session.exec(
            select(TQ_CIBD_REGIFINAPLAT)
            .where(TQ_CIBD_REGIFINAPLAT.FINAFFCODE == finaffcode)
            .distinct()
        ).all()
        if not results:
            logger.error(f"未找到融资归属地代码 '{finaffcode}' 的融资平台信息")
            return []
        logger.info(f"查询到 {len(results)} 条融资平台信息")
        return results
    except Exception as e:
        logger.error(f"查询地方融资平台信息时出错: {e}")
        return []

def get_same_affiliation_platforms(company_code: str, session: Session) -> List[TQ_CIBD_REGIFINAPLAT]:
    """
    根据公司代码查询融资归属地代码,然后查询同一个融资归属地的所有信息
    """
    try:
        logger.info(f"查询公司代码 '{company_code}' 的同一融资归属地平台信息")
        results = session.exec(
            select(TQ_CIBD_REGIFINAPLAT)
            .where(TQ_CIBD_REGIFINAPLAT.FINAFFCODE ==
                   select(TQ_CIBD_REGIFINAPLAT.FINAFFCODE)
                   .where(TQ_CIBD_REGIFINAPLAT.ITCODE == company_code)
                   .limit(1)
                   .scalar_subquery())
            .distinct()
        ).all()
        if not results:
            logger.error(f"未找到公司代码 '{company_code}' 的同一融资归属地平台信息")
            return []
        logger.info(f"查询到 {len(results)} 条同一融资归属地平台信息")
        return results
    except Exception as e:
        logger.error(f"查询同一融资归属地平台信息时出错: {e}")
        return []

def get_itcode_by_itname(itname: str, session: Session) -> str:
    """
    根据公司名称查询 ITCODE，如果不存在则抛异常
    """
    try:
        logger.info(f"查询公司名称 '{itname}' 的 ITCODE")
        result = session.exec(
            select(TQ_CIBD_REGIFINAPLAT.ITCODE)
            .where(TQ_CIBD_REGIFINAPLAT.ITNAME == itname)
            .distinct()
        ).first()
        if not result:
            raise Exception(f"{itname} 预警通未列为城投平台，无法生成。")
        return result
    except Exception as e:
        logger.error(f"查询 ITCODE 时出错: {e}")
        raise

if __name__ == "__main__":
    test_itcode = "81572830"
    with Session(engine) as session:
        try:
            # 测试用例 1：根据公司代码查询
            print("=== 测试一：根据公司代码查询 ===")
            result_itcode = get_regifinaplat_by_itcode(test_itcode, session)
            if result_itcode:
                print(f"公司代码: {result_itcode.ITCODE}")
                print(f"公司名称: {result_itcode.ITNAME}")
                print(f"注册地省份: {result_itcode.REGLANNAME_P}")
                print(f"注册地市县: {result_itcode.REGLANNAME_C}")
                print(f"融资归属地代码: {result_itcode.FINAFFCODE}")
                print(f"融资归属地: {result_itcode.FINAFFNAME}")
                print(f"归属地属性: {result_itcode.TERRITORYTYPE}")
                print(f"债券余额: {result_itcode.BONDBALANCE}")
            print("-" * 50)

            # 基于第一个查询结果获取 finaffcode，供后续测试使用
            finaffcode_for_test = result_itcode.FINAFFCODE if result_itcode else None

            # 测试用例 2：根据融资归属地代码查询（使用 81572830 对应的归属地代码）
            print("=== 测试二：根据融资归属地代码查询 ===")
            if finaffcode_for_test:
                results_finaffcode = get_regifinaplat_by_finaffcode(finaffcode_for_test, session)
                print(f"归属地代码 {finaffcode_for_test} 下共 {len(results_finaffcode)} 条记录")
                for item in results_finaffcode:
                    print(f"  公司代码: {item.ITCODE}, 公司名称: {item.ITNAME}, 债券余额: {item.BONDBALANCE}")
            else:
                print("无法获取归属地代码，跳过该用例")
            print("-" * 50)

            # 测试用例 3：查询同一融资归属地平台（以 81572830 为输入）
            print("=== 测试三：查询同一融资归属地平台 ===")
            results_same = get_same_affiliation_platforms(test_itcode, session)
            print(f"公司 {test_itcode} 同归属地共 {len(results_same)} 条记录")
            for item in results_same:
                print(f"  公司代码: {item.ITCODE}, 公司名称: {item.ITNAME}, 融资归属地: {item.FINAFFNAME}")
            print("-" * 50)

            # 新增测试用例 4：根据公司名称查询 ITCODE
            print("=== 测试四：根据公司名称查询 ITCODE ===")
            test_itname = "泰安泰山城乡建设发展有限公司"
            try:
                print(get_itcode_by_itname(test_itname, session))
            except Exception as e:
                print(f"查询失败: {e}")
            print("-" * 50)

        except Exception as e:
            print(f"错误: {e}")
