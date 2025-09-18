# Author: Junjun
# Date: 2025/5/19
import urllib.parse
from typing import List

from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker

from apps.datasource.models.datasource import DatasourceConf
from common.core.config import settings


def get_engine_config():
    return DatasourceConf(username=settings.POSTGRES_USER, password=settings.POSTGRES_PASSWORD,
                          host=settings.POSTGRES_SERVER, port=settings.POSTGRES_PORT, database=settings.POSTGRES_DB,
                          dbSchema="public", timeout=30) # read engine config


def get_engine_uri(conf: DatasourceConf):
    return f"postgresql+psycopg2://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{urllib.parse.quote(conf.database)}"


def get_engine_conn():
    conf = get_engine_config()
    db_url = get_engine_uri(conf)
    engine = create_engine(db_url,
                           connect_args={"options": f"-c search_path={conf.dbSchema}", "connect_timeout": conf.timeout},
                           pool_timeout=conf.timeout)
    return engine


def get_data_engine():
    engine = get_engine_conn()
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    return session


def create_table(session, table_name: str, fields: List[any]):
    # field type relation
    list = []
    comment_list = []

    # 生成字母序列
    def get_column_name(index):
        if index < 26:
            return chr(ord('A') + index)
        else:
            return chr(ord('A') + index // 26 - 1) + chr(ord('A') + index % 26)

    for i, f in enumerate(fields):
        if "object" in f["type"]:
            f["relType"] = "text"
        elif "int" in f["type"]:
            f["relType"] = "bigint"
        elif "float" in f["type"]:
            f["relType"] = "numeric"
        elif "datetime" in f["type"]:
            f["relType"] = "timestamp"
        else:
            f["relType"] = "text"
        # 使用字母作为列名
        column_name = get_column_name(i)
        list.append(f'"{column_name}" {f["relType"]}')

        # 保存原始列名作为注释
        comment_list.append(f'COMMENT ON COLUMN "{table_name}"."{column_name}" IS \'{f["name"]}\'')

    sql = f"""
            CREATE TABLE "{table_name}" (
                {", ".join(list)}
            );
            """
    # 添加注释语句
    comment_sql = ";\n".join(comment_list) + ";"

    session.execute(text(sql))
    session.execute(text(comment_sql))
    print(sql+comment_sql)
    session.commit()


def insert_data(session, table_name: str, fields: List[any], data: List[any]):
    engine = get_engine_conn()
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    with engine.connect() as conn:
        stmt = table.insert().values(data)
        conn.execute(stmt)
        conn.commit()
