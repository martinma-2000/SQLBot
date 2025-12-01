from apps.template.template import get_base_template


def get_sql_template():
    template = get_base_template()
    return template['template']['indicator_sql']

def get_indto_template():
    template = get_base_template()
    return template['template']['indicator_indto']
