import requests
import os
import pandas as pd
import datetime
import json
from typing import Dict, Optional
from excel_processing.excel_extract import ExcelHeaderProcessor
from sqlalchemy import create_engine, inspect

# 优先加载根目录与 backend 下的 .env，以支持直接运行脚本场景
_here = os.path.dirname(__file__)
_root_env = os.path.abspath(os.path.join(_here, '..', '..', '.env'))
_backend_env = os.path.abspath(os.path.join(_here, '..', '.env'))
try:
    from dotenv import load_dotenv
    for _p in (_root_env, _backend_env):
        if os.path.exists(_p):
            load_dotenv(_p, override=False)
except Exception:
    # 如果未安装 python-dotenv，则用简易解析器加载 .env 键值到进程环境
    def _manual_load_env(path: str):
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f:
                        s = line.strip()
                        if not s or s.startswith('#'):
                            continue
                        if '=' in s:
                            k, v = s.split('=', 1)
                            k = k.strip()
                            v = v.strip().strip('"').strip("'")
                            os.environ.setdefault(k, v)
        except Exception:
            pass
    _manual_load_env(_root_env)
    _manual_load_env(_backend_env)


def download_excel_to_bytes(url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None, timeout: int = 30) -> bytes | None:
    """
    从指定URL下载Excel字节内容，支持动态参数；不落地文件。

    参数:
    - url: 下载地址
    - params: 请求参数字典（键值与数量可变）
    - headers: 可选请求头
    - timeout: 请求超时时间（秒）
    返回:
    - bytes: Excel文件字节内容；失败返回None
    """
    try:
        resp = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
        resp.raise_for_status()
        print("√ Excel已下载为字节内容")
        return resp.content
    except requests.exceptions.RequestException as e:
        print(f"× 下载文件失败: {e}")
        return None


def process_excel_bytes(excel_bytes: bytes, sheet_name: int | str = 0) -> Optional[pd.DataFrame]:
    """
    使用ExcelHeaderProcessor处理字节内容，转换多级表头为单级表头。
    返回DataFrame，不写入本地。
    """
    try:
        processor = ExcelHeaderProcessor(separator="_")
        df = processor.convert_multi_to_single_header_filelike(excel_bytes, header_rows=None, sheet_name=sheet_name)
        print("√ Excel字节已处理为DataFrame")
        return df
    except Exception as e:
        print(f"× 处理Excel字节失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def insert_df_to_db(df: pd.DataFrame,
                    db_url: Optional[str] = None,
                    table_name: Optional[str] = None,
                    schema: Optional[str] = None,
                    if_exists: str = 'append',
                    dedup_keys: Optional[list[str]] = None,
                    dedup_with_db: Optional[bool] = None,
                    upsert_keys: Optional[list[str]] = None,
                    upsert_enabled: Optional[bool] = None,
                    upsert_create_unique: Optional[bool] = None) -> bool:
    """
    将DataFrame入库，数据库配置可通过参数或环境变量配置。

    环境变量（备选）:
    - DB_URL: 完整连接串，例如 postgres://... 或 mysql+pymysql://...
    - DB_TABLE_NAME: 目标表名
    - DB_SCHEMA: 目标schema（可选）
    - DB_IF_EXISTS: 'append' 或 'replace'
    去重、标准化与合并配置:
    - BI_DEDUP_KEYS: 逗号分隔的去重键，如 "机构,date_m"
    - BI_DEDUP_WITH_DB: 是否在数据库层面去重（true/false），默认 true
    - BI_ORG_COLUMN: 机构列名，默认智能检测：优先 "机构名称"，其次 "机构"，再环境变量
    - BI_ORG_ALIASES_JSON: JSON 字典，别名到规范名映射 {"别名":"规范名"}
    - BI_ORG_ALIASES_PATH: CSV 路径，包含两列 alias,canonical
    - BI_TIME_COLUMN: 时间列名（可选），默认智能检测：优先 "表格时期"，其次 "date_m"；若为日期/时间类型将统一格式为 YYYY-MM-DD
    合并（Upsert）配置：
    - BI_UPSERT_ENABLED: 是否启用合并（true/false），默认 true（仅 PostgreSQL 支持）
    - BI_UPSERT_KEYS: 逗号分隔的合并键，默认使用 [机构列,时间列]
    - BI_UPSERT_CREATE_UNIQUE: 是否创建唯一索引以支持冲突检测（true/false），默认 true
    """
    try:
        # 优先读取 BI 专用环境变量，避免影响系统库
        db_url = (
            db_url
            or os.getenv('BI_DB_URL')
            or os.getenv('SQLBOT_EXCEL_DB_URL')
            or os.getenv('DB_URL')
            or os.getenv('SQLBOT_DB_URL')
        )
        table_name = table_name or os.getenv('BI_TABLE_NAME') or os.getenv('DB_TABLE_NAME')
        schema = schema or os.getenv('BI_SCHEMA') or os.getenv('DB_SCHEMA')
        if_exists = os.getenv('BI_IF_EXISTS', os.getenv('DB_IF_EXISTS', if_exists)) or if_exists
        # 去重配置：支持环境变量 BI_DEDUP_KEYS（逗号分隔）与 BI_DEDUP_WITH_DB（true/false）
        if dedup_keys is None:
            _dedup_env = os.getenv('BI_DEDUP_KEYS')
            if _dedup_env:
                dedup_keys = [k.strip() for k in _dedup_env.split(',') if k.strip()]
        if dedup_with_db is None:
            dedup_with_db = (os.getenv('BI_DEDUP_WITH_DB', 'true').lower() in ('1', 'true', 'yes', 'y'))
        # Upsert 配置
        if upsert_enabled is None:
            upsert_enabled = (os.getenv('BI_UPSERT_ENABLED', 'true').lower() in ('1', 'true', 'yes', 'y'))
        if upsert_keys is None:
            _upsert_env = os.getenv('BI_UPSERT_KEYS')
            if _upsert_env:
                upsert_keys = [k.strip() for k in _upsert_env.split(',') if k.strip()]
        if upsert_create_unique is None:
            upsert_create_unique = (os.getenv('BI_UPSERT_CREATE_UNIQUE', 'true').lower() in ('1', 'true', 'yes', 'y'))

        # 严格分离：不再从系统 POSTGRES_* 环境变量拼接 BI 连接串，避免误写入系统库

        if not db_url:
            raise ValueError("缺少数据库连接字符串: 请传入 db_url 或设置环境变量 BI_DB_URL/SQLBOT_EXCEL_DB_URL/DB_URL/SQLBOT_DB_URL")
        if not table_name:
            raise ValueError("缺少目标表名: 请传入 table_name 或设置环境变量 DB_TABLE_NAME")

        engine = create_engine(db_url)

        # 入库前预处理：转换 Period/Categorical 等 psycopg 不支持的类型
        from pandas.api import types as ptypes
        df = df.copy()
        for _c in df.columns:
            _s = df[_c]
            try:
                if ptypes.is_period_dtype(_s):
                    # 将 Period 转为字符串（例如 '2025-03'），避免 psycopg 适配失败
                    df[_c] = _s.astype(str)
                elif isinstance(_s.dtype, pd.CategoricalDtype):
                    # 分类转为字符串，保持可写性
                    df[_c] = _s.astype(str)
            except Exception:
                # 忽略个别列类型探测异常，继续处理其他列
                pass
        # 字段标准化：机构名称与时间列统一，增强跨源去重效果
        try:
            # 机构标准化：智能检测列名
            _org_env = os.getenv('BI_ORG_COLUMN')
            _org_candidates = [_org_env, '机构名称', '机构']
            org_col = next((c for c in _org_candidates if c and c in df.columns), None)
            alias_map: Dict[str, str] = {}
            # 读取 JSON 映射
            _alias_json = os.getenv('BI_ORG_ALIASES_JSON')
            if _alias_json:
                try:
                    alias_map.update(json.loads(_alias_json))
                except Exception:
                    print('! BI_ORG_ALIASES_JSON 解析失败，忽略 JSON 映射')
            # 读取 CSV 映射
            _alias_path = os.getenv('BI_ORG_ALIASES_PATH')
            if _alias_path and os.path.exists(_alias_path):
                try:
                    _csv_df = pd.read_csv(_alias_path)
                    if {'alias', 'canonical'}.issubset(set(_csv_df.columns)):
                        alias_map.update({str(r['alias']): str(r['canonical']) for _, r in _csv_df.iterrows()})
                    else:
                        print('! BI_ORG_ALIASES_PATH 缺少列 alias,canonical，忽略 CSV 映射')
                except Exception:
                    print('! BI_ORG_ALIASES_PATH 读取失败，忽略 CSV 映射')

            def _to_halfwidth(s: str) -> str:
                # 全角转半角，去除常见空白与括号差异
                res = []
                for ch in s:
                    code = ord(ch)
                    if code == 12288:  # 全角空格
                        code = 32
                    elif 65281 <= code <= 65374:  # 全角字符范围
                        code -= 65248
                    res.append(chr(code))
                return ''.join(res)

            def _normalize_org_name(s: str) -> str:
                if not isinstance(s, str):
                    s = str(s)
                s = _to_halfwidth(s)
                s = s.replace('（', '(').replace('）', ')')
                s = s.strip()
                s = ' '.join(s.split())  # 规范空白
                return s

            if org_col and org_col in df.columns:
                df[org_col] = df[org_col].astype(str).map(lambda x: alias_map.get(_normalize_org_name(x), _normalize_org_name(x)))
            else:
                print(f"! 未找到机构列（已尝试 {[_org_env, '机构名称', '机构']}），跳过机构标准化")

            # 时间列标准化（可选）
            # 时间列标准化：智能检测列名
            _time_env = os.getenv('BI_TIME_COLUMN')
            _time_candidates = [_time_env, '表格日期', 'date_m']
            time_col = next((c for c in _time_candidates if c and c in df.columns), None)
            if time_col and time_col in df.columns:
                try:
                    if ptypes.is_datetime64_any_dtype(df[time_col]):
                        df[time_col] = pd.to_datetime(df[time_col], errors='coerce').dt.strftime('%Y-%m-%d')
                    elif ptypes.is_period_dtype(df[time_col]):
                        df[time_col] = df[time_col].astype(str)
                    else:
                        df[time_col] = df[time_col].astype(str).str.strip()
                except Exception as e:
                    print(f"! 时间列标准化失败，继续入库: {e}")
            elif time_col:
                print(f"! 未找到时间列（已尝试 {[_time_env, '表格日期', 'date_m']}），跳过时间标准化")
        except Exception as e:
            print(f"! 字段标准化过程异常，继续入库: {e}")
        # 内存层面去重：优先按 dedup_keys，否则按整行；
        # 保留首次出现，但时间列（表格日期_source/表格日期）按组保留最后值
        try:
            _before_cnt = len(df)
            time_cols = [c for c in ['表格日期_source', '表格日期'] if c in df.columns]
            if dedup_keys and all(k in df.columns for k in dedup_keys):
                # 保留首次的记录
                df_dedup = df.drop_duplicates(subset=dedup_keys, keep='first')
                # 时间列按组保留最后一次出现的值
                if time_cols:
                    last_times = df.groupby(dedup_keys, sort=False)[time_cols].last().reset_index()
                    df_dedup = df_dedup.merge(last_times, on=dedup_keys, how='left', suffixes=('', '_last'))
                    for c in time_cols:
                        cl = c + '_last'
                        if cl in df_dedup.columns:
                            df_dedup[c] = df_dedup[cl]
                    df_dedup.drop(columns=[c + '_last' for c in time_cols if (c + '_last') in df_dedup.columns], inplace=True)
                df = df_dedup
                print(f"√ 内存去重完成（保留首次，时间列取最后，keys={dedup_keys}）: {_before_cnt} -> {len(df)}")
            else:
                # 无明确去重键时，按整行保留首次
                df = df.drop_duplicates(keep='first')
                print(f"√ 内存去重完成（保留首次，整行）: {_before_cnt} -> {len(df)}")
        except Exception as e:
            print(f"! 内存去重失败，继续入库: {e}")
        try:
            insp = inspect(engine)
            existing_cols = set()
            try:
                existing_cols = {c['name'] for c in insp.get_columns(table_name, schema=schema)}
            except Exception:
                existing_cols = set()
            missing_cols = [c for c in df.columns if c not in existing_cols]

            def _infer_pg_type(series: pd.Series) -> str:
                # 使用 pandas.api.types 进行稳健的类型判断，避免已弃用 API
                if ptypes.is_bool_dtype(series):
                    return 'BOOLEAN'
                if ptypes.is_integer_dtype(series):
                    return 'BIGINT'
                if ptypes.is_float_dtype(series):
                    return 'DOUBLE PRECISION'
                if ptypes.is_datetime64_any_dtype(series):
                    return 'TIMESTAMP'
                if ptypes.is_period_dtype(series):
                    return 'TEXT'
                # 分类类型以 TEXT 存储
                try:
                    if isinstance(series.dtype, pd.CategoricalDtype):
                        return 'TEXT'
                except Exception:
                    pass
                # 字符串/对象统一 TEXT
                if ptypes.is_string_dtype(series) or ptypes.is_object_dtype(series):
                    return 'TEXT'
                return 'TEXT'

            if missing_cols:
                quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                with engine.begin() as conn:
                    for col in missing_cols:
                        pg_type = _infer_pg_type(df[col])
                        ddl = f'ALTER TABLE {quoted_table} ADD COLUMN "{col}" {pg_type};'
                        conn.exec_driver_sql(ddl)
                print(f"√ 已为现有表追加缺失列: {missing_cols}")
        except Exception as e:
            print(f"! 追加缺失列时遇到问题，继续写入: {e}")
        # 数据库层面去重：仅在 append 模式且提供 dedup_keys 时进行（启用 upsert 时跳过）
        try:
            if if_exists == 'append' and dedup_with_db and dedup_keys and not upsert_enabled:
                insp = inspect(engine)
                # 检查表与列存在性
                _existing_cols = set()
                try:
                    _existing_cols = {c['name'] for c in insp.get_columns(table_name, schema=schema)}
                except Exception:
                    _existing_cols = set()
                if not set(dedup_keys).issubset(_existing_cols):
                    print(f"! 数据库去重跳过：目标表缺少去重键 {dedup_keys}")
                else:
                    quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    cols_sql = ', '.join([f'"{k}"' for k in dedup_keys])
                    sql = f'SELECT {cols_sql} FROM {quoted_table}'
                    try:
                        existing_df = pd.read_sql_query(sql, con=engine)
                        existing_set = set(map(tuple, existing_df[dedup_keys].values.tolist()))
                        _before_cnt = len(df)
                        df = df[~df[dedup_keys].apply(tuple, axis=1).isin(existing_set)]
                        print(f"√ 数据库去重完成（keys={dedup_keys}）: {_before_cnt} -> {len(df)}")
                    except Exception as e:
                        print(f"! 数据库去重失败，继续入库: {e}")
        except Exception as e:
            print(f"! 数据库去重过程异常，继续入库: {e}")
        # 列位置保障：在完成去重校验后，删除旧时间列并在末尾重建（PostgreSQL）
        try:
            if db_url and db_url.startswith('postgresql'):
                insp = inspect(engine)
                quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                try:
                    _cols = insp.get_columns(table_name, schema=schema)
                except Exception:
                    _cols = []
                _existing_names = {c['name'] for c in _cols}
                _time_cols_df = [c for c in ['表格日期_source', '表格日期'] if c in df.columns]
                _to_drop = [c for c in _time_cols_df if c in _existing_names]
                if _to_drop:
                    with engine.begin() as conn:
                        for c in _to_drop:
                            conn.exec_driver_sql(f'ALTER TABLE {quoted_table} DROP COLUMN "{c}";')
                    print(f"√ 已删除旧时间列: {_to_drop}")
                # 在表末尾重建新时间列（TEXT），后续写入会填充新值
                _to_add = _time_cols_df
                if _to_add:
                    with engine.begin() as conn:
                        for c in _to_add:
                            conn.exec_driver_sql(f'ALTER TABLE {quoted_table} ADD COLUMN "{c}" TEXT;')
                    print(f"√ 已在末尾重建时间列: {_to_add}")
        except Exception as e:
            print(f"! 删除并重建时间列过程异常，继续入库: {e}")
        # PostgreSQL Upsert：合并 B/C 指标到同一行（按组合键）
        try:
            if upsert_enabled and db_url.startswith('postgresql'):
                # 选择 upsert 键：优先使用配置的 upsert_keys；若未配置，且机构+时间列可用，则用它们
                if not upsert_keys:
                    _auto_keys = []
                    # 尝试使用上面标准化时检测到的 org_col/time_col
                    try:
                        _org_env = os.getenv('BI_ORG_COLUMN')
                        _org_candidates = [_org_env, '机构名称', '机构']
                        org_col2 = next((c for c in _org_candidates if c and c in df.columns), None)
                    except Exception:
                        org_col2 = None
                    try:
                        _time_env = os.getenv('BI_TIME_COLUMN')
                        _time_candidates = [_time_env, '表格时期', 'date_m']
                        time_col2 = next((c for c in _time_candidates if c and c in df.columns), None)
                    except Exception:
                        time_col2 = None
                    if org_col2:
                        _auto_keys.append(org_col2)
                    if time_col2:
                        _auto_keys.append(time_col2)
                    if len(_auto_keys) >= 1:
                        upsert_keys = _auto_keys
                if upsert_keys and all(k in df.columns for k in upsert_keys):
                    quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    all_cols = list(df.columns)
                    key_cols = upsert_keys
                    non_key_cols = [c for c in all_cols if c not in key_cols]
                    cols_sql = ', '.join([f'"{c}"' for c in all_cols])
                    vals_sql = ', '.join([f'%({c})s' for c in all_cols])
                    keys_sql = ', '.join([f'"{k}"' for k in key_cols])
                    update_sql = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols]) or ''
                    sql = f'INSERT INTO {quoted_table} ({cols_sql}) VALUES ({vals_sql}) ON CONFLICT ({keys_sql}) DO UPDATE SET {update_sql};'
                    # 可选：创建唯一索引以支持冲突键
                    if upsert_create_unique and key_cols:
                        idx_name = f"uidx_{table_name}_{'_'.join([str(k) for k in key_cols])}".replace(' ', '_').lower()
                        create_idx_sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON {quoted_table} ({keys_sql});'
                        try:
                            with engine.begin() as conn:
                                conn.exec_driver_sql(create_idx_sql)
                            print(f"√ 已确保唯一索引存在: {idx_name} ON ({key_cols})")
                        except Exception as e:
                            print(f"! 创建唯一索引失败（忽略）: {e}")
                    # 执行逐行 upsert（稳妥，避免批量参数风格不兼容）
                    with engine.begin() as conn:
                        for rec in df.to_dict(orient='records'):
                            try:
                                conn.exec_driver_sql(sql, rec)
                            except Exception as e:
                                print(f"! Upsert 单行失败（跳过继续）: {e}")
                    print(f"√ Upsert 合并完成: {schema + '.' if schema else ''}{table_name}，按键 {upsert_keys}")
                    return True
                else:
                    print(f"! Upsert 跳过：未提供有效的合并键或键不在列中: {upsert_keys}")
            else:
                if upsert_enabled:
                    print("! Upsert 跳过：当前仅支持 PostgreSQL 连接串")
        except Exception as e:
            print(f"! Upsert 合并过程异常，退回常规写入: {e}")
        # 退回常规写入
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists=if_exists, index=False)
        print(f"√ DataFrame已入库: {schema + '.' if schema else ''}{table_name} ({if_exists})")
        return True
    except Exception as e:
        print(f"× DataFrame入库失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # 示例参数，可自由扩展/变更键值
    params = {
        "date_m": "2025-03-31",
        "p_date_m": "202503"
        # 可继续添加其他参数，如 "biz": "retail"
    }
    # 支持定时任务通过环境变量覆盖 URL
    url = os.getenv('JOB_URL') or 'http://127.0.0.1:8030/download_excel'

    excel_bytes = download_excel_to_bytes(url=url, params=params)
    if not excel_bytes:
        print("文件下载失败")
    else:
        df = process_excel_bytes(excel_bytes)
        if df is None:
            print("文件处理失败")
        else:
            # 入库配置可传参或通过环境变量：DB_URL/DB_TABLE_NAME/DB_SCHEMA/DB_IF_EXISTS
            ok = insert_df_to_db(
                df=df,
                db_url=(
                    os.getenv('BI_DB_URL')
                    or os.getenv('SQLBOT_EXCEL_DB_URL')
                    or os.getenv('DB_URL')
                    or os.getenv('SQLBOT_DB_URL')
                ),
                # 优先 BI_TABLE_NAME；退回 JOB_TABLE_NAME/DB_TABLE_NAME；默认 bi_excel_processed
                table_name=(
                    os.getenv('BI_TABLE_NAME')
                    or os.getenv('JOB_TABLE_NAME')
                    or os.getenv('DB_TABLE_NAME')
                    or 'bi_excel_processed'
                ),
                schema=os.getenv('BI_SCHEMA') or os.getenv('DB_SCHEMA'),
                if_exists=os.getenv('BI_IF_EXISTS', os.getenv('DB_IF_EXISTS', 'append'))
            )
            if ok:
                print("文件处理并入库完成")
            else:
                print("入库失败")
