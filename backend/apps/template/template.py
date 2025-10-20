import yaml

def load():
    with open('./template.yaml', 'r', encoding='utf-8') as f:
        return yaml.load(f, Loader=yaml.SafeLoader)


def get_base_template():
    return load()
