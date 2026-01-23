import re

from utils.connect_qwen import qwen_client
from utils.log_utils import get_logger
from config.llm_config import CURR_ENV,ENV_CONFIG
from utils.string_util import convert_val

# MODEL_PATH = 'Qwen2.5-72B-Instruct'

class PrivateApply:
    def __init__(self):
        self.logger = get_logger()
        self.client = qwen_client()

    # 批复信息
    def private_credit_approval(self, com, text, reply_date):
        self.logger.info('批复信息接口:')
        # self.logger.debug(text)
        llm_prompt = f"""
        - 银行授信批复内容：{text}
        - 银行授信批复时间：{reply_date}
        - 城投公司名称：{com}
        以上是银行授信批复内容、批复时间和城投公司名称，请返回一句话总结、提取并返回城投公司的授信批复信息，如果授信额度有多个用途需要逐个列举并尽量使用原文，授信额度用整数表示，不要返回其他内容，返回格式如：X年X月X日，我行同意给予X公司授信额度X万元，用于投资XX。
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user",
                 "content": llm_prompt}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return "\n".join([item.strip() for item in chat_response.choices[0].message.content.split("\n") if item])
        # return chat_response.choices[0].message.content.strip()

    # 拟投债券要素接口，获取拟投债券要素，需要将第一段内容也融合进来
    def private_para4(self, text):
        self.logger.info('拟投债券要素接口:')
        # self.logger.debug(text)

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user",
                 "content": "请根据下面的文字，总结债券名称、发行人、争议解决方式、发行规模、发行期限、票面利率、还本付息方式、外部评级、主承销商、承销方式、担保方式、募集资金用途，其余信息无需展示，每样信息占用一行，信息缺失的写无,主承销商不包含联席主承销商，票面（票面利率）也可以用询价区间值，外部评级使用主体评级，发行期限取最具体的信息（如果有）而不取期限限制信息，发行期限优先从【授信申请方案】获取（尽量使用原文数据：" + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return "\n".join([item.strip() for item in chat_response.choices[0].message.content.split("\n") if item])
        # return chat_response.choices[0].message.content.strip()

    # 发行人概况
    def private_faxing_condition(self, text):
        self.logger.info('发行人概况接口:')
        # self.logger.debug(text)

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user",
                 "content": "根据以下文字，帮我总结发行人成立日期、注册资本、实收资本、外部评级和持股情况，以几句话总结，不要换行展示，其余信息无需展示。"
                            "内容格式如下所示：发行人***（机构全称）成立于***（时间），注册资本**元，实收资本**元。外部主体评级**。\n文本内容：" + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    def get_equity_structure_description(self, search_results: list) -> str:
        """
        从募集说明书中检索股权结构相关内容并生成描述

        Args:
            search_results: 通过关键词检索到的股权结构相关内容列表

        Returns:
            str: 股权结构描述文本
        """
        if not search_results or len(search_results) == 0:
            self.logger.debug("未检索到股权结构相关内容")
            return ""

        try:
            equity_summary_prompt = [{
                "role": "user",
                "content": f"""/no_think
                请从以下内容中提取股权结构信息，按以下格式输出：
                
                发行人控股股东为【股东名称】，持股比例为【XX%】，实际控制人为【实际控制人名称】。
                
                【检索内容】
                {str(search_results)[:10000]}
                
                【要求】
                1. 直接输出一句话，不需要标题或其他说明
                2. 如某项信息缺失，该部分省略不写
                3. 不要添加任何额外描述
                """
            }]

            response = self.client.chat.completions.create(
                model=ENV_CONFIG.get(CURR_ENV).get("model"),
                messages=equity_summary_prompt,
                temperature=0.2,
                max_tokens=600
            )

            equity_description = response.choices[0].message.content.strip()

            # 清理可能的格式标记
            if equity_description.startswith("```"):
                equity_description = equity_description.split("```")[
                    1] if "```" in equity_description else equity_description

            if equity_description and len(equity_description) > 10:
                self.logger.info(f"成功生成股权结构描述，长度: {len(equity_description)}")
                return equity_description
            else:
                self.logger.warning("生成的股权结构描述内容过短")
                return ""

        except Exception as e:
            self.logger.exception(f"生成股权结构描述失败: {e}")
            return ""

        # 发债平台排名

    def private_fazhai_rank(self, text):
        self.logger.info('发债平台排名接口:')
        # self.logger.debug(text)
        prompt_gaikuang = "请根据下面的发债平台情况，总结发行人总资产和排名情况，只用一句话展示"

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_gaikuang + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 债券余额
    def private_zhaiquan_yue(self, text):
        self.logger.info('债券余额接口:')
        # self.logger.debug(text)
        prompt_gaikuang = ("请根据提供的余额情况，总结发行人债券存量数目、存量规模（对应债券余额之和）和公募债（对应募集方式为公募债券的债券余额之和）、公募债占存量规模的比例情况."
                           "并按照如下格式输出,'截至**年*月，发行人债券存量数目一共*只，存量规模为**亿元。其中，公募债券余额为**亿元，占存量规模的*%。"
                           "提供的余额情况如下:")

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_gaikuang + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()




        # 获取营业收入情况

    def private_zhaiquan_yue_v1(self, text):
        self.logger.info('债券余额接口:')
        # self.logger.debug(text)
        prompt_gaikuang = ("根据表格数据，提取债券只数，存量规模，公募债券，公募债券占比."
                           "并按照如下格式输出,'截至最新，发行人债券存量数目一共*只，存量规模为**亿元。其中，公募债券余额为**亿元，占存量规模的*%。"
                           "提供的表格数据如下:")

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_gaikuang + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

        # 获取营业收入情况

    def private_5para_yinye_shouru(self, text):
        self.logger.info('营业收入情况接口:')

        prompt = f"""
        你的任务是从给定的表格数据中总结营业收入情况，你的工作流如下：
        1. 判断表格数据的时间范围，近三年 或 近两年及一期。
        2. 提取 主营业务收入 信息。如果数据中的金额单位是万（元）时，通过执行下面python代码，将所有金额转换为亿（元）。
        3. 将 主营业务收入 按照时间由远到近排序。
        4. 根据上述信息进行总结，总结应尽可能简洁，以段落的形式呈现。不要换行，不要输出思考过程，直接返回文本，在总结中使用转换后的金额和单位，不要再出现旧的金额和单位。
        
        表格数据如下：
        {text}

        ```python
        def convert(val):
            val = float(val.replace(",",""))
            conv = round(val / 10000, 2)
            return val
        ```
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 获取发行人营业情况总结
    def private_5para_yinye_summary(self, text):
        self.logger.info('营业情况总结接口:')
        # self.logger.debug(text)

        prompt_text = """
        你的任务是根据提供的内容，对营业情况进行总结。你的工作流如下：
        1. 识别出 完整年度中 主营业务收入 金额大于1亿元的 主营业务。
        2. 根据 识别的主营业务 进行总结，总结的内容包含 主营业务板块总结-业务模式-经营情况。
        请直接输出总结，不要输出任何其他内容。总结应尽可能完整，以段落的形式呈现。

        提供的内容如下：
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 获取发行人总资产情况，对应第六章第一、二段内容

    def private_6para_first(self, text):
        self.logger.info('获取发行人总资产情况接口:')
        prompt_text = f"""
        【资产负债表】
        {text}
        【流动资产各项】
        ["货币资金","结算备付金","拆出资金","应收保证金","应收利息","应收票据","应收账款"
            ,"应收款项融资","应收保费","应收分保账款","应收分保合同准备金","应收出口退税","应收补贴款"
            ,"内部应收款","预付款项","其他应收款","存货","买入返售金融资产","交易性金融资产","衍生金融资产"
            ,"一年内到期的非流动资产","待处理流动资产损益","其他流动资产"]
        【非流动资产各项】
        ["发放贷款及垫款","可供出售金融资产"
            ,"划分为持有待售的资产","以公允价值计量且其变动计入其他综合收益的金融资产","以摊余成本计量的金融资产"
            ,"债权投资","其他债权投资","其他权益工具投资","其他非流动金融资产","长期应收款","长期股权投资","待摊费用"
            ,"其他长期投资","投资性房地产","固定资产","合同资产","在建工程","使用权资产","工程物资"
            ,"生产性生物资产","公益性生物资产","油气资产","无形资产","开发支出","商誉","长期待摊费用"
            ,"股权分置流通权","递延所得税资产","其他非流动资产"]

        请根据提供的【资产负债表】表格数据，描述最新该公司情况，需要遵循以下规则：
        1、如果数据中的金额单位是万元时，将所有金额转换为亿元。
        2.从【资产负债表】表格中提取资产合计。
        3、根据【流动资产各项】和【非流动资产各项】分别筛选资产不小于1的各项（禁止遗漏一亿和一点多亿的数据）。
        4、根据转换后的数据和单位以及步骤2和步骤3的结果进行总结，不要输出思考过程，直接返回文本，总结的样例如下所示，分三段话展示：
            截至**年**月末，发行人总资产为**亿元，其中流动资产为**亿元，占总资产的**%。\n
            流动资产中主要包括货币资金**亿元、结算备付金**亿元（依次罗列所有流动资产不小于1的各项）。\n
            非流动资产方面，主要包括发放贷款及垫款**亿元、可供出售金融资产**亿元（依次罗列非流动资产金额不小于1的各项）。
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        res = chat_response.choices[0].message.content.strip()
        return re.sub(r"\n+", "\n", res)
        # return chat_response.choices[0].message.content.strip()

    # 获取对应第六章受限资产情况

    def private_6para_shouxian(self, text):
        self.logger.info('获取对应第六章受限资产情况接口:')
        # self.logger.debug(text)
        prompt_text = "请根据下面的表格数据，展示受限资产合计数据，限制20个字，金额为亿元，其他信息无需展示："

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 获取发行人总资产情况，对应第六章总负债情况

    def private_6para_fuzhai(self, text):
        self.logger.info('获取发行人总负债情况接口:')
        prompt_text = f"""
        【资产负债表】
        {text}
        【负债各项】
        ["短期借款", "向中央银行借款", "吸收存款及同业存放", "拆入资金", "交易性金融负债"
        , "衍生金融负债", "卖出回购金融资产款", "应付手续费及佣金", "应付票据", "应付账款", "预收款项"
        , "合同负债", "应付职工薪酬", "应交税费", "应付利息", "应付股利", "其他应交款", "应付保证金"
        , "内部应付款", "其他应付款", "预提费用", "预计流动负债", "应付分保账款", "保险合同准备金"
        , "代理买卖证券款", "代理承销证券款", "国际票证结算", "国内票证结算", "一年内的递延收益", "应付短期债券"
        , "一年内到期的非流动负债", "其他流动负债", "长期借款", "长期应付职工薪酬", "应付债券"
        , "应付债券：优先股","长期应付款", "预计非流动负债", "长期递延收益"
        , "递延所得税负债", "其他非流动负债", "租赁负债", "担保责任赔偿准备金", "划分为持有待售的负债"]

        你的任务是根据提供的【资产负债表】表格以及【负债各项】，总结发行人最新一期的负债情况，你的工作流如下：
            1.如果数据中的金额单位是万元或者是元，需要将所有金额转换为亿元。
            2.从【资产负债表】表格中提取负债合计。
            3.根据【负债各项】列表，从【资产负债表】表格中提取所有负债项目及负债金额，然后计算并筛选出负债金额不小于1的所有项目，禁止遗漏。
            4.根据转换后的数据和单位以及步骤2和步骤3的筛选结果进行总结，总结一句话文本，输出思考过程，并将总结文本放在最后，总结中不要有思考描述。例如：截至**年**月末(如果有时间信息，如果是0x月，转换为x月)，发行人总负债**亿元，资产负债率**%。其中短期借款**亿元、向中央银行借款**亿元(依次罗列所有负债金额不小于1的各项)。
        """
        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip().split("\n")[-1].strip("总结：")

    # 获取发行人有息负债情况，对应第六章有息负债

    def private_6para_youxifuzhai(self, text, fuzhai_total):
        self.logger.info('获取发行人有息负债情况接口:')
        # prompt_text = f"""
        # 以下是发行人有息负债和总负债情况，请提取相关信息：
        # - {text}
        # 要求：
        # 1. 提取短期有息负债金额，数据所在表头可能为一年期以内、一年以内（含1年）等，直接取最后的合计数据，将金额从万元转换为亿元。
        # 2. 提取最新一年的有息负债总额，数据所在表头可能为xxxx年x月末、xxxx年末、合计等，并将单位从万元转换为亿元。
        # 3. 提取总负债中的负债总额，将金额从万元转换为亿元。
        # 4. 生成数据校验步骤：
        #    - 核对步骤1,2的数据结果，如果结果相等则为提取错误，需要重新执行步骤1,2。
        #    - 步骤1的结果必须小于步骤2的结果，否则为提取错误，需要重新执行步骤1,2。
        # 5. 生成计算步骤：
        #    - 显示所有的计算过程，包括转换过程和占比计算。
        #    - 确保计算过程正确，且结果最终展示在总结中。
        # 6. 生成一段总结,字数50字以内，可参考格式：截至**年**月末(如果有时间信息，如果是0x月，转换为x月)，发行人有息负债总额为X亿元，占总负债比例Y%。短期债务金额为A亿元，在有息负债中占比为B%。
        # 注意：以上数字可能包含千位逗号（例如"xxx,xxx.xx"），请将千位逗号去掉并进行正确的数值计算，例如：ab,cde.fg万元，去掉千位逗号为abcde.fg，将万元转为亿元a.bc亿元，即abcde.fg/10000=a.bc
        # 请确保所有数字格式正确，计算准确，结果应包括计算步骤与总结。
        # """
        # prompt_text = f"""
        # 以下是发行人有息负债情况和总负债合计，请提取相关信息：
        # - 有息负债情况: {text}
        # - 总负债合计: {fuzhai_total}
        # 要求：
        # 1. 提取短期有息负债金额，数据所在表头可能为一年期以内、一年以内（含1年）等，直接取最后的合计数据，将金额从万元转换为亿元。
        # 2. 提取最新一年的有息负债总额，数据所在表头可能为xxxx年x月末、xxxx年末、合计等，并将单位从万元转换为亿元。
        # 3. 生成数据校验步骤：
        #    - 核对步骤1,2的数据结果，如果结果相等则为提取错误，需要重新执行步骤1,2。
        #    - 步骤1的结果必须小于步骤2的结果，否则为提取错误，需要重新执行步骤1,2。
        # 4. 生成计算步骤：
        #    - 显示所有的计算过程，包括转换过程和占比计算。
        #    - 确保计算过程正确，且结果最终展示在总结中。
        # 5. 生成一段总结,字数50字以内，可参考格式：截至**年**月末(如果有时间信息，如果是0x月，转换为x月)，发行人有息负债总额为X亿元，占总负债比例Y%。短期债务金额为A亿元，在有息负债中占比为B%。
        # 注意：以上数字可能包含千位逗号（例如"xxx,xxx.xx"），请将千位逗号去掉并进行正确的数值计算，例如：ab,cde.fg万元，去掉千位逗号为abcde.fg，将万元转为亿元a.bc亿元，即abcde.fg/10000=a.bc
        # 请确保所有数字格式正确，计算准确，结果应包括计算步骤与总结。
        # """
        prompt_text = f"""
        以下是发行人有息负债情况和总负债合计，请提取相关信息：
        - 有息负债情况: {text}
        - 总负债合计: {fuzhai_total}
        要求：
        1. 根据总负债合计的统计时间和有息负债的时间序列，提取两者都有的最新一年的有息负债总额，数据所在表头可能为xxxx年x月末、xxxx年末、合计等，并将单位从万元转换为亿元。根据结果及对应总负债合计，计算有息负债占总负债比例。
        2. 提取债务期限不超过一年的债务金额，数据所在表头可能为'一年期以内'、'一年以内（含1年）'、'一年以内'、'一年内到期金额'等，数据一般在表格末尾的合计中，将金额从万元转换为亿元。根据结果和步骤1中的有息负债总额计算短期有息负债占有息负债总额的比例。
        3. 生成数据校验步骤：
           - 核对步骤1,2提取的数据结果，如果结果相等则为提取错误，需要重新执行步骤1,2。
           - 步骤1提取的结果必须小于步骤2提取的结果，否则为提取错误，需要重新执行步骤1,2。
        4. 生成计算步骤：
           - 显示所有的计算过程，包括转换过程和占比计算。
           - 确保计算过程正确，且结果最终展示在总结中。
        5. 生成一段总结,字数50字以内，可参考格式：截至**年**月末(如果有时间信息，如果是0x月，转换为x月)，发行人有息负债总额为X亿元，占总负债比例Y%。短期债务金额为A亿元，在有息负债中占比为B%。
        注意：以上数字可能包含千位逗号（例如"xxx,xxx.xx"），请将千位逗号去掉并进行正确的数值计算，例如：ab,cde.fg万元，去掉千位逗号为abcde.fg，将万元转为亿元a.bc亿元，即abcde.fg/10000=a.bc
        请确保所有数字格式正确，计算准确，结果应包括计算步骤与总结。
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        tmp_result = chat_response.choices[0].message.content.strip()
        summary_start = tmp_result.find("#### 5. 总结")
        summary_start1 = tmp_result.find("### 总结")
        # 如果找到了“### 总结”标签
        if summary_start != -1:
            # 截取从“### 总结”开始到文本结尾的部分
            summary_text = tmp_result[summary_start + len("#### 5. 总结"):].strip()
            return summary_text
        elif summary_start1 != -1:
            # 截取从“### 总结”开始到文本结尾的部分
            summary_text = tmp_result[summary_start1 + len("### 总结"):].strip()
            return summary_text
        else:
            print(tmp_result)
            return tmp_result # None

    # 获取发行人授信额度

    def private_6para_shouxinedu(self, text):
        self.logger.info('获取发行人授信额度接口:')

        prompt_text = f"""
        你的任务是根据提供的表格，进行内容总结，你的工作流如下：
        1.如果数据中的金额单位是万（元）时，通过执行下面python代码，将所有金额转换为亿（元）
        2.根据转换后的数据和单位，总结一句话文本，不要换行，不要输出思考过程，直接返回文本，例如："截至**年**月末(如果有时间信息，如果是0x月，转换为x月)，发行人授信总额度为**亿元，已使用**亿元，剩余未使用额度为**亿元。"。在总结中使用转换后的金额和单位，不要再出现旧的金额和单位
        
        ```python
        def convert(val):
            val = float(val.replace(",",""))
            conv = round(val / 10000, 2)
            return conv
        ```
        """
        # f"""
        # 你的任务是根据提供的表格，进行内容总结，你的工作流如下：
        # 1.如果数据中的金额单位是万（元）时，通过执行下面python代码，将所有金额转换为亿（元）
        # 2.根据转换后的数据和单位，总结发行人最新一年的对外担保情况，以一段文本展示，不要换行，直接返回文本，包括对外担保余额、占当期末总资产的比例等情况，字数控制在200字左右。在总结中使用转换后的金额和单位，不要再出现旧的金额和单位
        # """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()
        # res = chat_response.choices[0].message.content.strip()
        # match = re.findall("【(.*?)】", res)
        # if match:
        #     return match[0]

    # 现金流情况统计

    def private_6para_xianjinliu(self, text):
        self.logger.info('现金流情况统计接口:')
        # self.logger.debug(text) 1.确定现金流数据的【时间范围】，若含有月份，现金流的时间范围为“近两年及一期”，否则为“近三年”；

        # prompt_gaikuang = """
        # 你的任务是根据提供的表格数据，对发行人现金流情况进行总结，你的工作流如下：
        # 1.确定现金流数据的时间范围，若时间维度都为年末，现金流的时间范围为“近三年”，否则为“近两年及一期”；
        # 2.提取 经营活动净现金流、投资活动净现金流、筹资活动净现金流情况 等关键信息，单位为亿元.
        # 3.将提取的数据按照时间从早到晚排序。
        # 4.结合上面的信息进行一句话总结, 数据时间按照从早到晚排序。回答尽可能简洁，总结格式如：近三年(步骤1中的时间范围)，发行人经营活动产生的现金流量净额分别为**亿元、**亿元、**亿元；投资活动产生的现金流量净额分别为**亿元、**亿元、**亿元；筹资活动产生的现金流量净额分别为**亿元、**亿元、**亿元。
        # 下面是表格数据：
        # """
        prompt_gaikuang = """
        你的任务是根据提供的表格数据，对发行人现金流情况进行总结，你的工作流如下：
        1.确定现金流数据的时间范围。若有三个时间维度且都为年末则表示为近三年；若有两个年末一个月末则表示为近两年及一期。
        2.提取 经营活动净现金流、投资活动净现金流、筹资活动净现金流情况 等关键信息，单位为亿元.
        3.将提取的数据按照时间从早到晚排序。
        4.结合上面的信息进行一句话总结, 数据时间按照从早到晚排序。回答尽可能简洁，总结格式如：近三年(步骤1中的结果)，发行人经营活动产生的现金流量净额分别为**亿元、**亿元、**亿元；投资活动产生的现金流量净额分别为**亿元、**亿元、**亿元；筹资活动产生的现金流量净额分别为**亿元、**亿元、**亿元。
        下面是表格数据：
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_gaikuang + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 对外担保情况

    def private_6para_danbao(self, text):
        self.logger.info('对外担保情况接口:')

        prompt_text = f"""
        你的任务是根据提供的表格，进行内容总结，你的工作流如下：
        1.如果数据中的金额单位是万（元）时，通过执行下面python代码，将所有金额转换为亿（元）
        2.根据转换后的数据和单位，总结一句话文本，不要换行，不要输出思考过程，直接返回文本，例如："截至**年**月末(如果有时间信息，如果是0x月，转换为x月)，发行人对外担保余额**亿元，占净资产的比例为xx%，被担保人主要为当地国企，代偿风险不高。"。在总结中使用转换后的金额和单位，不要再出现旧的金额和单位
        
        ```python
        def convert(val):
            val = float(val.replace(",",""))
            conv = round(val / 10000, 2)
            return conv
        ```
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_text + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 被执行人情况

    def private_6para_beizhixingren(self, text):
        self.logger.info('被执行人情况接口:')
        # self.logger.debug(text)

        prompt_beizhixingren = "请根据下面的数据，如果没有数据则直接返回'无被执行人情况数据'；如果有数据则总结欠款人被执行金额情况，时间无需说明，以一段文本展示，字数控制在200字以内，格式如下所示：发行人期末大额应收账款中，欠款人某某公司，经企查查查询，均被列为被执行人，被执行金额分别为多少钱："

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_beizhixingren + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        return chat_response.choices[0].message.content.strip()

    # 保证人情况

    def private_6para_baozhengren(self, text):
        self.logger.info('保证人情况接口:')
        prompt_gaikuang = """
        你的任务是根据提供的保证人信息对其基本情况进行总结，你的工作流如下：
        1.提取 保证人名称。
        2.提取 保证人成立时间、注册资本、实收资本、控股情况、最新主体评级、总资产排名情况、保证人债券余额信息 等关键信息。
        3.对提取到的信息进行总结，总结尽量简短，注意语句通顺。
        4.如果有多个保证人，则分多段展示。如：
            ```
            xxx
            xxx
            ```
        请直接返回总结后的文本，不要有其他任何内容。

        下面是保证人信息：
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_gaikuang + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        # return chat_response.choices[0].message.content.strip()
        return "\n".join([item.strip() for item in chat_response.choices[0].message.content.split("\n") if item])

    def private_6para_fengxiandian(self, text):
        self.logger.info('风险点接口:')
        # 总结可参考下面示例
        prompt_gaikuang = """
        你的任务是根据提供的财务风险信息进行总结，只总结风险现状，不要总结原因等其他内容，你的工作流如下：
        1.查看债务压力，看是否有存在短期有息债务占比高、非标融资占比高的风险。如果存在，则一句话总结。
        2.查看资产流动，看是否有存在存货占比高、其他应收占比高、流动资产占比低的风险。如果存在，则一句话总结。
        3.总结要符合逻辑，前后匹配，尤其是时间序列和数据要一一对应。可参考下面示例。如果提供的信息中没有以上两类风险信息，直接返回结果“无”。
        【示例】
        1、资产流动性差：截至2025年3月末，发行人总资产为634.35亿元，其中流动资产为224.19亿元，占总资产的35.34%。
        2、存货占比高：截至2025年3月末，发行人总资产为576.39亿元，其中存货326.71亿元，占总资产比例56.68%。

        下面是风险信息：
        """

        chat_response = self.client.chat.completions.create(
            model=ENV_CONFIG.get(CURR_ENV).get("model"),
            messages=[
                {"role": "user", "content": prompt_gaikuang + text}
            ],
            temperature=0.2,
            top_p=0.9,
            max_tokens=8192,
            stream=False
        )
        # return chat_response.choices[0].message.content.strip()
        res = "\n".join([item.replace(". ", "、").replace(".", "、").replace("、 ", "、").strip() for item in chat_response.choices[0].message.content.split("\n") if item])
        return convert_val(res)

    def generate(self, prompts, temperature=0):
        try:
            response = self.client.chat.completions.create(
                model=ENV_CONFIG.get(CURR_ENV).get("model"),
                messages=prompts,
                temperature=temperature,
                response_format={"type": "json_object"},
                #timeout=30
            )
            return response.choices[0].message.content
            # return response.choices[0].message.reasoning_content
        except Exception as e:
            raise Exception(f"An error occurred while calling OpenAI API: {e}")




if __name__ == "__main__":
    apply = PrivateApply()
    text = "[{'name': '发行主体评级', 'date': '202412', 'header_rows': 1, 'unit': '', 'data': [['公司名称', '主体评级'], ['江苏句容福地生态科技有限公司', 'AA+']]}, '注册名称:江苏句容福地生态科技有限公司', '法定代表人:陈创', '注册资本:人民币102,000.00万元', '实缴资本:人民币102,000.00万元', '设立（工商注册）日期:1998年1月16日', '统一社会信用代码:91321183799093948X', '住所（注册地）:江苏省镇江市句容市华阳镇华阳东路16号', '邮政编码:212400', '所属行业:《上市公司行业分类指引》（2012年修订）：E47房屋建筑业', '经营范围:农业生态观光园区的开发、建设；农业产业化的投资建设；乡镇集镇建设；市政公用设施、道路基础设施的投资建设；经营管理市政府授权范围内的国有资产；土地收储、整理、开发；旅游资源开发；城乡建设；水利基础设施建设。（依法须经批准的项目，经相关部门批准后方可开展经营活动）一般项目：农产品的生产、销售、加工、运输、贮藏及其他相关服务；农业专业及辅助性活动（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）', '电话及传真号码:电话：0511-87301701传真号码：0511-87301703', '信息披露事务负责人及其职位与联系方式:信息披露事务负责人：张胥职位：董事联系方式：0511-85172815','金融市场部申请给予江苏句容福地生态科技有限公司主体授信额度4000万元，专项用于债券投资，本期债券投资拟从一级认购，债券1：江苏句容福地生态科技有限公司2025年面向专业投资者非公开发行公司债券（第一期），品种为非公开公司债，期限5年，票面2.5-3.5%，主承销商为华泰联合证券。额度可调剂用于相同发行人发行的其他品种债券投资（短融、超短融、公开发行公司债、非公开发行公司债、企业债、中票、PPN等，根据具体业务需求选取相应品种）。', '根据《关于明确我行债券投资业务申报流程及审批授权的通知》授权方案，本笔业务审批路径为上贷审会。', [['金额', '期限', '价格', '品种'], ['不超过3亿', '5年', '2.5-3.5%', '非公开公司债']], '历史合作情况:', [['债券简称', '投资金额（万元）', '利率', '债券起息日', '到期日', '实际转出日'], ['19句容03', '9,000.00', '7.00%', '2019/11/12', '2024/11/12', '2020/3/31'], ['19句容03', '4,000.00', '7.00%', '2019/11/12', '2024/11/12', '2020/4/2'], ['19句容03', '4,000.00', '7.00%', '2019/11/12', '2024/11/12', '2021/11/12']]]"
    result = apply.private_faxing_condition(text)
    print(result)