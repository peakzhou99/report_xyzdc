import json
import difflib
import copy
import time
import re

from utils.log_utils import get_logger

logger = get_logger()


class TableProcessor:
    def __init__(self, table_contexts, llm):
        """
        [{"id":"","menu":"","pretext":"","content":"","darray":[[]],"oxml":object}]
        """
        self.table_contexts = table_contexts
        self.llm = llm

    # 生成表格特征
    def gen_table_feature(self, table_type_list: list):
        start_time = time.time()
        table_feature_datas = []
        for table_context in self.table_contexts:
            base_feature_messages = [{
                "role": "user",
                "content": f"""/no_think
                你是财务表格分析专家，根据表格所属章节、表格上下文和表格内容，提取表格特征，并以JSON格式输出：
                【表格特征】
                表格类型，所属章节，表格标题，表头，关键词，时间序列
                【表格特征提取说明】
                表格类型：
                - 仅限以下类型范围：{table_type_list}
                - 若表格类型不在类型范围内，强制输出**其他表**，不得保留原类型名称
                关键词
                - 关键词要包含期限信息（如果有）
                【表格所属章节】
                {table_context.get("menu")}
                【表格上下文】
                {table_context.get("pretext")}
                【表格内容】
                {table_context.get("content")}
                【输出格式】
                {{
                    "表格类型":"",
                    "所属章节":"",
                    "表格标题":"",
                    "表头":[],
                    "关键词":[],
                    "时间序列":[],
                }}
                【特别说明】
                输出结果严格按照JSON格式输出，且表格特征相关字段必须都要输出，已经尝试{{itr_cnt}}次，不能再出错了。
                """
            }]
            tmp_table_type = None
            feature_obj = {}
            cnt = 0
            while tmp_table_type is None and cnt < 3:
                cnt += 1
                feature_messages = copy.deepcopy(base_feature_messages)
                feature_messages[0]['content'] = self.safe_format(feature_messages[0]['content'], itr_cnt=cnt)
                # print("表格上下文数据：",table_context)
                try:
                    result = self.llm.generate(feature_messages)
                    feature_obj = json.loads(result)
                    # if feature_obj.get("表格类型",None) in (table_type_list+['其他表']):
                    if feature_obj.get("表格类型", None) is not None and feature_obj.get("表格类型", None) != "":
                        tmp_table_type = feature_obj.get("表格类型")
                except:
                    logger.error(f"特征提取失败：{table_context.get('pretext')}")

                if cnt > 1:
                    logger.debug(f"表格特征提取轮次{cnt}")
                    logger.debug("菜单：", table_context.get("menu"))
                    logger.debug("表格上下文：", table_context.get("pretext"))
                    logger.debug(f"表格特征：{result}")
                    logger.debug(f"表格数据：{table_context.get('content')}")

            table_feature_datas.append({"id": table_context.get("id"), **feature_obj})
        return table_feature_datas

    def _get_tables_by_ids(self, table_ids):
        target_table_datas = []
        for table_context in self.table_contexts:
            table_name = next(
                (table_item.get("name") for table_item in table_ids if table_item.get("id") == table_context.get("id")),
                None)
            if table_name:
                new_table_context = table_context.copy()
                new_table_context["name"] = table_name
                target_table_datas.append(new_table_context)
                if len(target_table_datas) == len(table_ids):
                    break
        return target_table_datas

    def _extract_target_table_id(self, target_table_desc, candidate_table_features):
        base_target_messages = [{
            "role": "user",
            "content": f"""/no_think
            你是财务表格分析专家，根据表格特征列表，判断哪个表格特征最符合目标表，将表格特征id以JSON格式输出：
            【表格特征列表】
            {candidate_table_features}
            【目标表】
            {target_table_desc}
            【结果输出】
            {{"id":""}}
            【异常输出】
            {{"id":"0"}}
            【特别说明】
            输出结果严格按照JSON格式输出，且表格特征相关字段必须都要输出，已经尝试{{itr_cnt}}次，不能再出错了。
            """
        }]
        target_id = None
        cnt = 0
        while target_id is None and cnt < 3:
            cnt += 1
            target_messages = copy.deepcopy(base_target_messages)
            target_messages[0]['content'] = self.safe_format(target_messages[0]['content'], itr_cnt=cnt)
            if cnt > 1:
                print(f"目标表描述：{target_table_desc}")
                print("候选表特征：")
                for candidate_table_feature in candidate_table_features:
                    print(candidate_table_feature)
            target_result = self.llm.generate(target_messages)
            try:
                target_id = json.loads(target_result).get("id", "0")
                if not re.match(r'^[a-z0-9\-]{4,}$', target_id):
                    target_id = None
            except:
                print(f"目标表格识别异常:目标表描述：{target_table_desc} \n 候选表格特征：{candidate_table_features}")
        return target_id

    def extract_target_tables(self, target_table_descs, table_feature_datas):
        extract_start_time = time.time()
        # 目标表ID
        target_table_ids = []
        for tab_name, tab_desc in target_table_descs.items():
            candidate_table_features = []
            for table_feature_data in table_feature_datas:
                table_feature_obj = table_feature_data
                if difflib.SequenceMatcher(isjunk=None, a=tab_name,
                                           b=table_feature_obj.get("表格类型", "")).ratio() > 0.8:
                    candidate_table_features.append(table_feature_data)
            if len(candidate_table_features) == 0:
                continue
            # 调用大模型
            table_id = self._extract_target_table_id(tab_desc, candidate_table_features)
            if table_id:
                target_table_ids.append({"name": tab_name, "id": table_id})
        if len(target_table_ids) == 0:
            return []
        target_table_datas = self._get_tables_by_ids(target_table_ids)
        return target_table_datas

    def safe_format(self, template: str, **kwargs):
        return re.sub(
            r'\{\s*([a-zA-Z0-9_])\s*\}',
            lambda m: str(kwargs.get(m.group(1).strip(), m.group(0))),
            template
        )
