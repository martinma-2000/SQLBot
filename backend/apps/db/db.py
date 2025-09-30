import base64
import json
import platform
import urllib.parse
from decimal import Decimal
from typing import Optional

import psycopg2
import pymssql
from apps.db.db_sql import get_table_sql, get_field_sql, get_version_sql
from common.error import ParseSQLResultError

if platform.system() != "Darwin":
    import dmPython
import pymysql
import redshift_connector
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker

from apps.datasource.models.datasource import DatasourceConf, CoreDatasource, TableSchema, ColumnSchema
from apps.datasource.utils.utils import aes_decrypt
from apps.db.constant import DB, ConnectType
from apps.db.engine import get_engine_config
from apps.system.crud.assistant import get_ds_engine
from apps.system.schemas.system_schema import AssistantOutDsSchema
from common.core.deps import Trans
from common.utils.utils import SQLBotLogUtil
from fastapi import HTTPException
from apps.db.es_engine import get_es_connect, get_es_index, get_es_fields, get_es_data_by_http


def get_uri(ds: CoreDatasource) -> str:
    conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration))) if ds.type != "excel" else get_engine_config()
    return get_uri_from_config(ds.type, conf)


def get_uri_from_config(type: str, conf: DatasourceConf) -> str:
    db_url: str
    if type == "mysql":
        if conf.extraJdbc is not None and conf.extraJdbc != '':
            db_url = f"mysql+pymysql://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}?{conf.extraJdbc}"
        else:
            db_url = f"mysql+pymysql://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}"
    elif type == "sqlServer":
        if conf.extraJdbc is not None and conf.extraJdbc != '':
            db_url = f"mssql+pymssql://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}?{conf.extraJdbc}"
        else:
            db_url = f"mssql+pymssql://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}"
    elif type == "pg" or type == "excel":
        if conf.extraJdbc is not None and conf.extraJdbc != '':
            db_url = f"postgresql+psycopg2://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}?{conf.extraJdbc}"
        else:
            db_url = f"postgresql+psycopg2://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}"
    elif type == "oracle":
        if conf.mode == "service_name":
            if conf.extraJdbc is not None and conf.extraJdbc != '':
                db_url = f"oracle+oracledb://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}?service_name={conf.database}&{conf.extraJdbc}"
            else:
                db_url = f"oracle+oracledb://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}?service_name={conf.database}"
        else:
            if conf.extraJdbc is not None and conf.extraJdbc != '':
                db_url = f"oracle+oracledb://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}?{conf.extraJdbc}"
            else:
                db_url = f"oracle+oracledb://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}"
    elif type == "ck":
        if conf.extraJdbc is not None and conf.extraJdbc != '':
            db_url = f"clickhouse+http://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}?{conf.extraJdbc}"
        else:
            db_url = f"clickhouse+http://{urllib.parse.quote(conf.username)}:{urllib.parse.quote(conf.password)}@{conf.host}:{conf.port}/{conf.database}"
    else:
        raise 'The datasource type not support.'
    return db_url


def get_extra_config(conf: DatasourceConf):
    config_dict = {}
    if conf.extraJdbc:
        config_arr = conf.extraJdbc.split("&")
        for config in config_arr:
            kv = config.split("=")
            if len(kv) == 2 and kv[0] and kv[1]:
                config_dict[kv[0]] = kv[1]
            else:
                raise Exception(f'param: {config} is error')
    return config_dict


def get_origin_connect(type: str, conf: DatasourceConf):
    extra_config_dict = get_extra_config(conf)
    if type == "sqlServer":
        return pymssql.connect(
            server=conf.host,
            port=str(conf.port),
            user=conf.username,
            password=conf.password,
            database=conf.database,
            timeout=conf.timeout,
            tds_version='7.0',  # options: '4.2', '7.0', '8.0' ...,
            **extra_config_dict
        )


# use sqlalchemy
def get_engine(ds: CoreDatasource, timeout: int = 0) -> Engine:
    conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration))) if ds.type != "excel" else get_engine_config()
    if conf.timeout is None:
        conf.timeout = timeout
    if timeout > 0:
        conf.timeout = timeout
    if ds.type == "pg":
        if conf.dbSchema is not None and conf.dbSchema != "":
            engine = create_engine(get_uri(ds),
                                   connect_args={"options": f"-c search_path={urllib.parse.quote(conf.dbSchema)}",
                                                 "connect_timeout": conf.timeout},
                                   pool_timeout=conf.timeout)
        else:
            engine = create_engine(get_uri(ds),
                                   connect_args={"connect_timeout": conf.timeout},
                                   pool_timeout=conf.timeout)
    elif ds.type == 'sqlServer':
        engine = create_engine('mssql+pymssql://', creator=lambda: get_origin_connect(ds.type, conf),
                               pool_timeout=conf.timeout)
    elif ds.type == 'oracle':
        engine = create_engine(get_uri(ds),
                               pool_timeout=conf.timeout)
    else:  # mysql, ck
        engine = create_engine(get_uri(ds), connect_args={"connect_timeout": conf.timeout}, pool_timeout=conf.timeout)
    return engine


def get_session(ds: CoreDatasource | AssistantOutDsSchema):
    engine = get_engine(ds) if isinstance(ds, CoreDatasource) else get_ds_engine(ds)
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    return session


def check_connection(trans: Optional[Trans], ds: CoreDatasource | AssistantOutDsSchema, is_raise: bool = False):
    if isinstance(ds, CoreDatasource):
        db = DB.get_db(ds.type)
        if db.connect_type == ConnectType.sqlalchemy:
            conn = get_engine(ds, 10)
            try:
                with conn.connect() as connection:
                    SQLBotLogUtil.info("success")
                    return True
            except Exception as e:
                SQLBotLogUtil.error(f"Datasource {ds.id} connection failed: {e}")
                if is_raise:
                    raise HTTPException(status_code=500, detail=trans('i18n_ds_invalid') + f': {e.args}')
                return False
        else:
            conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration)))
            extra_config_dict = get_extra_config(conf)
            if ds.type == 'dm':
                with dmPython.connect(user=conf.username, password=conf.password, server=conf.host,
                                      port=conf.port, **extra_config_dict) as conn, conn.cursor() as cursor:
                    try:
                        cursor.execute('select 1', timeout=10).fetchall()
                        SQLBotLogUtil.info("success")
                        return True
                    except Exception as e:
                        SQLBotLogUtil.error(f"Datasource {ds.id} connection failed: {e}")
                        if is_raise:
                            raise HTTPException(status_code=500, detail=trans('i18n_ds_invalid') + f': {e.args}')
                        return False
            elif ds.type == 'doris':
                with pymysql.connect(user=conf.username, passwd=conf.password, host=conf.host,
                                     port=conf.port, db=conf.database, connect_timeout=10,
                                     read_timeout=10, **extra_config_dict) as conn, conn.cursor() as cursor:
                    try:
                        cursor.execute('select 1')
                        SQLBotLogUtil.info("success")
                        return True
                    except Exception as e:
                        SQLBotLogUtil.error(f"Datasource {ds.id} connection failed: {e}")
                        if is_raise:
                            raise HTTPException(status_code=500, detail=trans('i18n_ds_invalid') + f': {e.args}')
                        return False
            elif ds.type == 'redshift':
                with redshift_connector.connect(host=conf.host, port=conf.port, database=conf.database,
                                                user=conf.username,
                                                password=conf.password,
                                                timeout=10, **extra_config_dict) as conn, conn.cursor() as cursor:
                    try:
                        cursor.execute('select 1')
                        SQLBotLogUtil.info("success")
                        return True
                    except Exception as e:
                        SQLBotLogUtil.error(f"Datasource {ds.id} connection failed: {e}")
                        if is_raise:
                            raise HTTPException(status_code=500, detail=trans('i18n_ds_invalid') + f': {e.args}')
                        return False
            elif ds.type == 'kingbase':
                with psycopg2.connect(host=conf.host, port=conf.port, database=conf.database,
                                      user=conf.username,
                                      password=conf.password,
                                      connect_timeout=10, **extra_config_dict) as conn, conn.cursor() as cursor:
                    try:
                        cursor.execute('select 1')
                        SQLBotLogUtil.info("success")
                        return True
                    except Exception as e:
                        SQLBotLogUtil.error(f"Datasource {ds.id} connection failed: {e}")
                        if is_raise:
                            raise HTTPException(status_code=500, detail=trans('i18n_ds_invalid') + f': {e.args}')
                        return False
            elif ds.type == 'es':
                es_conn = get_es_connect(conf)
                if es_conn.ping():
                    SQLBotLogUtil.info("success")
                    return True
                else:
                    SQLBotLogUtil.info("failed")
                    return False
    else:
        conn = get_ds_engine(ds)
        try:
            with conn.connect() as connection:
                SQLBotLogUtil.info("success")
                return True
        except Exception as e:
            SQLBotLogUtil.error(f"Datasource {ds.id} connection failed: {e}")
            if is_raise:
                raise HTTPException(status_code=500, detail=trans('i18n_ds_invalid') + f': {e.args}')
            return False

    return False


def get_version(ds: CoreDatasource | AssistantOutDsSchema):
    version = ''
    conf = None
    if isinstance(ds, CoreDatasource):
        conf = DatasourceConf(
            **json.loads(aes_decrypt(ds.configuration))) if ds.type != "excel" else get_engine_config()
    if isinstance(ds, AssistantOutDsSchema):
        conf = DatasourceConf()
        conf.host = ds.host
        conf.port = ds.port
        conf.username = ds.user
        conf.password = ds.password
        conf.database = ds.dataBase
        conf.dbSchema = ds.db_schema
        conf.timeout = 10
    db = DB.get_db(ds.type)
    sql = get_version_sql(ds, conf)
    try:
        if db.connect_type == ConnectType.sqlalchemy:
            with get_session(ds) as session:
                with session.execute(text(sql)) as result:
                    res = result.fetchall()
                    version = res[0][0]
        else:
            extra_config_dict = get_extra_config(conf)
            if ds.type == 'dm':
                with dmPython.connect(user=conf.username, password=conf.password, server=conf.host,
                                      port=conf.port) as conn, conn.cursor() as cursor:
                    cursor.execute(sql, timeout=10, **extra_config_dict)
                    res = cursor.fetchall()
                    version = res[0][0]
            elif ds.type == 'doris':
                with pymysql.connect(user=conf.username, passwd=conf.password, host=conf.host,
                                     port=conf.port, db=conf.database, connect_timeout=10,
                                     read_timeout=10, **extra_config_dict) as conn, conn.cursor() as cursor:
                    cursor.execute(sql)
                    res = cursor.fetchall()
                    version = res[0][0]
            elif ds.type == 'redshift' or ds.type == 'es':
                version = ''
    except Exception as e:
        print(e)
        version = ''
    return version.decode() if isinstance(version, bytes) else version


def get_schema(ds: CoreDatasource):
    conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration))) if ds.type != "excel" else get_engine_config()
    db = DB.get_db(ds.type)
    if db.connect_type == ConnectType.sqlalchemy:
        with get_session(ds) as session:
            sql: str = ''
            if ds.type == "sqlServer":
                sql = """select name from sys.schemas"""
            elif ds.type == "pg" or ds.type == "excel":
                sql = """SELECT nspname FROM pg_namespace"""
            elif ds.type == "oracle":
                sql = """select * from all_users"""
            with session.execute(text(sql)) as result:
                res = result.fetchall()
                res_list = [item[0] for item in res]
                return res_list
    else:
        extra_config_dict = get_extra_config(conf)
        if ds.type == 'dm':
            with dmPython.connect(user=conf.username, password=conf.password, server=conf.host,
                                  port=conf.port, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute("""select OBJECT_NAME from dba_objects where object_type='SCH'""", timeout=conf.timeout)
                res = cursor.fetchall()
                res_list = [item[0] for item in res]
                return res_list
        elif ds.type == 'redshift':
            with redshift_connector.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                            password=conf.password,
                                            timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute("""SELECT nspname FROM pg_namespace""")
                res = cursor.fetchall()
                res_list = [item[0] for item in res]
                return res_list
        elif ds.type == 'kingbase':
            with psycopg2.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                  password=conf.password,
                                  options=f"-c statement_timeout={conf.timeout * 1000}",
                                  **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute("""SELECT nspname FROM pg_namespace""")
                res = cursor.fetchall()
                res_list = [item[0] for item in res]
                return res_list


def get_tables(ds: CoreDatasource):
    conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration))) if ds.type != "excel" else get_engine_config()
    db = DB.get_db(ds.type)
    sql, sql_param = get_table_sql(ds, conf, get_version(ds))
    if db.connect_type == ConnectType.sqlalchemy:
        with get_session(ds) as session:
            with session.execute(text(sql), {"param": sql_param}) as result:
                res = result.fetchall()
                res_list = [TableSchema(*item) for item in res]
                return res_list
    else:
        extra_config_dict = get_extra_config(conf)
        if ds.type == 'dm':
            with dmPython.connect(user=conf.username, password=conf.password, server=conf.host,
                                  port=conf.port, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql, {"param": sql_param}, timeout=conf.timeout)
                res = cursor.fetchall()
                res_list = [TableSchema(*item) for item in res]
                return res_list
        elif ds.type == 'doris':
            with pymysql.connect(user=conf.username, passwd=conf.password, host=conf.host,
                                 port=conf.port, db=conf.database, connect_timeout=conf.timeout,
                                 read_timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql, (sql_param,))
                res = cursor.fetchall()
                res_list = [TableSchema(*item) for item in res]
                return res_list
        elif ds.type == 'redshift':
            with redshift_connector.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                            password=conf.password,
                                            timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql, (sql_param,))
                res = cursor.fetchall()
                res_list = [TableSchema(*item) for item in res]
                return res_list
        elif ds.type == 'kingbase':
            with psycopg2.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                  password=conf.password,
                                  options=f"-c statement_timeout={conf.timeout * 1000}",
                                  **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql.format(sql_param))
                res = cursor.fetchall()
                res_list = [TableSchema(*item) for item in res]
                return res_list
        elif ds.type == 'es':
            res = get_es_index(conf)
            res_list = [TableSchema(*item) for item in res]
            return res_list


def get_fields(ds: CoreDatasource, table_name: str = None):
    conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration))) if ds.type != "excel" else get_engine_config()
    db = DB.get_db(ds.type)
    sql, p1, p2 = get_field_sql(ds, conf, table_name)
    if db.connect_type == ConnectType.sqlalchemy:
        with get_session(ds) as session:
            with session.execute(text(sql), {"param1": p1, "param2": p2}) as result:
                res = result.fetchall()
                res_list = [ColumnSchema(*item) for item in res]
                return res_list
    else:
        extra_config_dict = get_extra_config(conf)
        if ds.type == 'dm':
            with dmPython.connect(user=conf.username, password=conf.password, server=conf.host,
                                  port=conf.port, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql, {"param1": p1, "param2": p2}, timeout=conf.timeout)
                res = cursor.fetchall()
                res_list = [ColumnSchema(*item) for item in res]
                return res_list
        elif ds.type == 'doris':
            with pymysql.connect(user=conf.username, passwd=conf.password, host=conf.host,
                                 port=conf.port, db=conf.database, connect_timeout=conf.timeout,
                                 read_timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql, (p1, p2))
                res = cursor.fetchall()
                res_list = [ColumnSchema(*item) for item in res]
                return res_list
        elif ds.type == 'redshift':
            with redshift_connector.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                            password=conf.password,
                                            timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql, (p1, p2))
                res = cursor.fetchall()
                res_list = [ColumnSchema(*item) for item in res]
                return res_list
        elif ds.type == 'kingbase':
            with psycopg2.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                  password=conf.password,
                                  options=f"-c statement_timeout={conf.timeout * 1000}",
                                  **extra_config_dict) as conn, conn.cursor() as cursor:
                cursor.execute(sql.format(p1, p2))
                res = cursor.fetchall()
                res_list = [ColumnSchema(*item) for item in res]
                return res_list
        elif ds.type == 'es':
            res = get_es_fields(conf, table_name)
            res_list = [ColumnSchema(*item) for item in res]
            return res_list


def exec_sql(ds: CoreDatasource | AssistantOutDsSchema, sql: str, origin_column=False):
    while sql.endswith(';'):
        sql = sql[:-1]

    db = DB.get_db(ds.type)
    if db.connect_type == ConnectType.sqlalchemy:
        with get_session(ds) as session:
            with session.execute(text(sql)) as result:
                try:
                    columns = result.keys()._keys if origin_column else [item.lower() for item in result.keys()._keys]
                    res = result.fetchall()
                    result_list = [
                        {str(columns[i]): float(value) if isinstance(value, Decimal) else value for i, value in
                         enumerate(tuple_item)}
                        for tuple_item in res
                    ]
                    return {"fields": columns, "data": result_list,
                            "sql": bytes.decode(base64.b64encode(bytes(sql, 'utf-8')))}
                except Exception as ex:
                    raise ParseSQLResultError(str(ex))
    else:
        conf = DatasourceConf(**json.loads(aes_decrypt(ds.configuration)))
        extra_config_dict = get_extra_config(conf)
        if ds.type == 'dm':
            with dmPython.connect(user=conf.username, password=conf.password, server=conf.host,
                                  port=conf.port, **extra_config_dict) as conn, conn.cursor() as cursor:
                try:
                    cursor.execute(sql, timeout=conf.timeout)
                    res = cursor.fetchall()
                    columns = [field[0] for field in cursor.description] if origin_column else [field[0].lower() for
                                                                                                field in
                                                                                                cursor.description]
                    result_list = [
                        {str(columns[i]): float(value) if isinstance(value, Decimal) else value for i, value in
                         enumerate(tuple_item)}
                        for tuple_item in res
                    ]
                    return {"fields": columns, "data": result_list,
                            "sql": bytes.decode(base64.b64encode(bytes(sql, 'utf-8')))}
                except Exception as ex:
                    raise ParseSQLResultError(str(ex))
        elif ds.type == 'doris':
            with pymysql.connect(user=conf.username, passwd=conf.password, host=conf.host,
                                 port=conf.port, db=conf.database, connect_timeout=conf.timeout,
                                 read_timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    res = cursor.fetchall()
                    columns = [field[0] for field in cursor.description] if origin_column else [field[0].lower() for
                                                                                                field in
                                                                                                cursor.description]
                    result_list = [
                        {str(columns[i]): float(value) if isinstance(value, Decimal) else value for i, value in
                         enumerate(tuple_item)}
                        for tuple_item in res
                    ]
                    return {"fields": columns, "data": result_list,
                            "sql": bytes.decode(base64.b64encode(bytes(sql, 'utf-8')))}
                except Exception as ex:
                    raise ParseSQLResultError(str(ex))
        elif ds.type == 'redshift':
            with redshift_connector.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                            password=conf.password,
                                            timeout=conf.timeout, **extra_config_dict) as conn, conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    res = cursor.fetchall()
                    columns = [field[0] for field in cursor.description] if origin_column else [field[0].lower() for
                                                                                                field in
                                                                                                cursor.description]
                    result_list = [
                        {str(columns[i]): float(value) if isinstance(value, Decimal) else value for i, value in
                         enumerate(tuple_item)}
                        for tuple_item in res
                    ]
                    return {"fields": columns, "data": result_list,
                            "sql": bytes.decode(base64.b64encode(bytes(sql, 'utf-8')))}
                except Exception as ex:
                    raise ParseSQLResultError(str(ex))
        elif ds.type == 'kingbase':
            with psycopg2.connect(host=conf.host, port=conf.port, database=conf.database, user=conf.username,
                                  password=conf.password,
                                  options=f"-c statement_timeout={conf.timeout * 1000}",
                                  **extra_config_dict) as conn, conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    res = cursor.fetchall()
                    columns = [field[0] for field in cursor.description] if origin_column else [field[0].lower() for
                                                                                                field in
                                                                                                cursor.description]
                    result_list = [
                        {str(columns[i]): float(value) if isinstance(value, Decimal) else value for i, value in
                         enumerate(tuple_item)}
                        for tuple_item in res
                    ]
                    return {"fields": columns, "data": result_list,
                            "sql": bytes.decode(base64.b64encode(bytes(sql, 'utf-8')))}
                except Exception as ex:
                    raise ParseSQLResultError(str(ex))
        elif ds.type == 'es':
            try:
                res, columns = get_es_data_by_http(conf, sql)
                columns = [field.get('name') for field in columns] if origin_column else [field.get('name').lower() for
                                                                                          field in
                                                                                          columns]
                result_list = [
                    {str(columns[i]): float(value) if isinstance(value, Decimal) else value for i, value in
                     enumerate(tuple(tuple_item))}
                    for tuple_item in res
                ]
                return {"fields": columns, "data": result_list,
                        "sql": bytes.decode(base64.b64encode(bytes(sql, 'utf-8')))}
            except Exception as ex:
                raise Exception(str(ex))
