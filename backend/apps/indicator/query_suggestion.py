# apps/indicator/query_suggestion.py
from apps.template.generate_indicator_indto.generator import get_sql_template


def generate_sql_prompt(org_code: str, rag_retrieved: str, question: str):
    """
    Generate query prompt based on user input and context
    """
    sql_sys_question = get_sql_template()['system'].format()
    sql_user_question = get_sql_template()['user'].format(org_code=org_code, 
                                                          rag_retrieved=rag_retrieved, 
                                                          user_question=question)
    return sql_sys_question, sql_user_question

def generate_indto_prompt(generated_sql):
    """
    Optimize existing query prompt based on feedback
    """
    indto_sys_question = get_sql_template()['system'].format()
    indto_user_question = get_sql_template()['user'].format()
    return indto_sys_question, indto_user_question
