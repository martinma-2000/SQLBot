# apps/indicator/coordinator.py

from .query_suggestion import generate_sql_prompt, generate_indto_prompt
from .sql_generator import generate_sql_from_prompt
from .parameter_generator import generate_input_parameters
from .data_fetcher import call_data_api


def execute_indicator_pipeline(user_query: str, org_code, rag_retrieved) -> dict:
    """
    Coordinate all modules to complete indicator execution pipeline
    """

    org_code = "000000"
    rag_retrieved = "指标信息片段"

    try:
        # 1. 根据用户问题、指标信息片段、机构名称及编码生成系统提示词及用户提示词
        sql_sys_prompt, sql_user_prompt = generate_sql_prompt(org_code, rag_retrieved, user_query)

        # 2. 调用 LLM 生成 SQL
        generated_sql = generate_sql_from_prompt(sql_sys_prompt, sql_user_prompt)

        # 3. 根据生成的 SQL 构造系统提示词及用户提示词
        # 这里可以根据SQL内容进一步优化参数生成的提示词
        indto_sys_prompt, indto_user_prompt = generate_indto_prompt(generated_sql)

        # 4. 调用 LLM 生成 API 的入参
        api_params = generate_input_parameters(indto_sys_prompt, indto_user_prompt)

        # 5. 调用 API 获取数据
        result = call_data_api(api_params)

        return {
            "success": True,
            "data": result,
            "sql": generated_sql,
            "parameters": api_params
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": None
        }

