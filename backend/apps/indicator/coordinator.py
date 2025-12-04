# apps/indicator/coordinator.py

from apps.indicator.query_suggestion import generate_sql_prompt, generate_indto_prompt
from apps.indicator.sql_generator import generate_sql_from_prompt
from apps.indicator.parameter_generator import generate_input_parameters
from apps.indicator.data_fetcher import call_data_api
from common.utils.utils import SQLBotLogUtil


def execute_indicator_pipeline(user_query: str, org_code: str='', rag_retrieved: str=''):
    """
    Coordinate all modules to complete indicator execution pipeline
    """

    SQLBotLogUtil.info('入参信息：')
    SQLBotLogUtil.info(f'user_query -> {user_query}')
    SQLBotLogUtil.info(f'org_code -> {org_code}')
    SQLBotLogUtil.info(f'rag_retrieved -> {rag_retrieved}')

    try:
        # 1. 根据用户问题、指标信息片段、机构名称及编码生成系统提示词及用户提示词
        sql_sys_prompt, sql_user_prompt = generate_sql_prompt(org_code, rag_retrieved, user_query)

        # 2. 调用 LLM 生成 SQL
        generated_sql = generate_sql_from_prompt(sql_sys_prompt, sql_user_prompt)
        SQLBotLogUtil.info('已生成的SQL是：' + '*'*30)
        SQLBotLogUtil.info(generated_sql)
        SQLBotLogUtil.info('*' * 60)

        # 3. 根据生成的 SQL 构造系统提示词及用户提示词
        # 这里可以根据SQL内容进一步优化参数生成的提示词
        indto_sys_prompt, indto_user_prompt = generate_indto_prompt(generated_sql)


        # 4. 调用 LLM 生成 API 的入参
        api_params = generate_input_parameters(indto_sys_prompt, indto_user_prompt)
        SQLBotLogUtil.info('生成的入参内容如下：' + '*'*30)
        SQLBotLogUtil.info(api_params)
        SQLBotLogUtil.info('*' * 60)

        # 5. 调用 API 获取数据
        SQLBotLogUtil.info('START 调用API取数' + '*'*30)
        try:
            result = call_data_api(api_params)
        except Exception as e:
            result = ""

        SQLBotLogUtil.info('END 调用API取数' + '*'*30)

        return {
            "success": True,
            "data": result,
            "sql": generated_sql,
            "parameters": api_params
        }
    except Exception as e:
        SQLBotLogUtil.error(str(e))
        return {
            "success": False,
            "error": str(e),
            "data": None
        }

if __name__ == "__main__":
    print('*' * 60)
    user_query = "收单业务按支付渠道交易方式统计月交易笔数上半年的趋势如何？"
    org_code = "27050814"
    rag_retrieved = """
{
              "idx_ecd/指标编号": "ELC_03670",
              "指标名称": "收单业务按支付渠道交易方式统计月交易笔数",
              "dmns_cd1/维度1枚举值及含义": "010101:条码微信主扫，010102:条码微信被扫，010201:条码支付宝主扫，010202:条码支付宝被扫，010301:条码本行主扫，010302:条>码本行被扫，010401:条码银联主扫，010402:条码银联被扫，020101:网关微信主扫，020102:网关微信被扫，020201:网关支付宝主扫，020202:网关支付宝被扫，020301:网关本行主扫，020302:网关本行被扫，020401:网关银联主扫，020402:网关银联被扫，030101:银行卡本行借记，030102:银行卡本行贷记，030201:银行卡他行借记，030202:银行卡他行贷记，040101:O2O本月，040102:O2O本年，050101:信用卡扫码，050102:信用卡刷卡",
              "dmns_cd2/维度2枚举值及含义": "",
              "dmns_cd3/维度3枚举值及含义": "",
              "dmns_cd4/维度4枚举值及含义": "",
              "dmns_cd5/维度5枚举值及含义": ""
          }
"""

    execute_indicator_pipeline(user_query, org_code, rag_retrieved)

    print('=' * 60)
