# apps/indicator/query_suggestion.py

from datetime import datetime
from apps.template.generate_indicator_indto.generator import get_sql_template, get_indto_template
from common.utils.utils import SQLBotLogUtil


def generate_sql_prompt(org_code: str, rag_retrieved: str, question: str):
    """
    Generate query prompt based on user input and context
    """

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    SQLBotLogUtil.info('Generating sql prompt ... ...')
    sql_sys_question = get_sql_template()['system'].format()
    sql_user_question = get_sql_template()['user'].format(current_time='2025-12-02 11:00:00',
                                                          org_code=org_code, 
                                                          rag_retrieved=rag_retrieved, 
                                                          user_query=question)
    SQLBotLogUtil.info('Generated SQL Prompt is:' + '*'*30)
    SQLBotLogUtil.info(sql_sys_question)
    SQLBotLogUtil.info(sql_user_question)
    SQLBotLogUtil.info('*' * 60)

    return sql_sys_question, sql_user_question

def generate_indto_prompt(generated_sql):
    """
    Optimize existing query prompt based on feedback
    """
    SQLBotLogUtil.info('Generating indto prompt ... ...')
    indto_sys_question = get_indto_template()['system'].format()
    indto_user_question = get_indto_template()['user'].format(generated_sql=generated_sql)
    SQLBotLogUtil.info('Generated INDTO Prompt is:' + '*'*30)
    SQLBotLogUtil.info(indto_sys_question)
    SQLBotLogUtil.info(indto_user_question)
    SQLBotLogUtil.info('*' * 60)

    return indto_sys_question, indto_user_question
