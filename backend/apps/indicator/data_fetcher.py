# apps/indicator/data_fetcher.py

import re
import json


def call_data_api(sql: str, params: dict = None) -> dict:
    """
    Call data API with SQL and parameters to fetch data
    """
    pass

def format_api_params(params: str) -> str:
    """
    Format parameters for API call
    """
    # 如果输入是markdown格式的SQL代码块，提取其中的SQL内容
    if params.startswith("```sql") and params.endswith("```"):
        # 移除markdown代码块标记
        lines = params.strip().split('\n')
        if len(lines) > 2:
            # 移除第一行(```sql)和最后一行(```)
            sql_content = '\n'.join(lines[1:-1])
        else:
            # 只有开始和结束标记的情况
            sql_content = ""
    else:
        # 如果不是markdown格式，直接使用原始内容
        sql_content = params

    # 将json中的 \" 转换为 \\\"
    formatted_sql = sql_content.replace('\\"', '\\\\"')

    # 移除 \n 换行符
    formatted_sql = formatted_sql.replace('\n', '')

    return formatted_sql

def format_api_response(response: dict) -> dict:
    """
    Format API response for further processing
    """
    pass
