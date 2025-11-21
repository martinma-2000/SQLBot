import requests
import os
import pandas as pd
import datetime
import json
from typing import Dict, Optional
from io import BytesIO
from excel_processing.excel_extract import ExcelHeaderProcessor
from excel_processing.enrich_city_from_org import enrich_city_column
from sqlalchemy import create_engine, inspect
import sqlalchemy as sa

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
        return resp.content
    except requests.exceptions.RequestException as e:
        return None


def process_excel_bytes(excel_bytes: bytes, sheet_name: int | str = 0) -> Optional[pd.DataFrame]:
    """
    使用ExcelHeaderProcessor处理字节内容，转换多级表头为单级表头。
    返回DataFrame，不写入本地。
    """
    try:
        processor = ExcelHeaderProcessor(separator="_")
        df = processor.convert_multi_to_single_header_filelike(excel_bytes, header_rows=None, sheet_name=sheet_name)
        return df
    except Exception as e:
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

    环境变量（仅 BI_*）:
    - BI_DB_URL: 完整连接串，例如 postgres://... 或 mysql+pymysql://...
    - BI_TABLE_NAME: 目标表名（未设置时默认 'bi_excel_processed'）
    - BI_SCHEMA: 目标 schema（可选）
    - BI_IF_EXISTS: 'append' 或 'replace'
    去重、标准化与合并配置:
    - BI_DEDUP_KEYS: 逗号分隔的去重键，如 "机构,date_m"
    - BI_DEDUP_WITH_DB: 是否在数据库层面去重（true/false），默认 true
    - BI_ORG_COLUMN: 机构列名，默认智能检测：优先 "机构名称"，其次 "机构"，再环境变量
    - BI_ORG_ALIASES_JSON: JSON 字典，别名到规范名映射 {"别名":"规范名"}
    - BI_ORG_ALIASES_PATH: CSV 路径，包含两列 alias,canonical
    - BI_TIME_COLUMN: 时间列名（可选），默认智能检测：优先 "表格日期"，其次 "date_m"；若为日期/时间类型将统一格式为 YYYY-MM-DD
    合并（Upsert）配置：
    - BI_UPSERT_ENABLED: 是否启用合并（true/false），默认 true（仅 PostgreSQL 支持）
    - BI_UPSERT_KEYS: 逗号分隔的合并键，默认使用 [机构列,时间列]
    - BI_UPSERT_CREATE_UNIQUE: 是否创建唯一索引以支持冲突检测（true/false），默认 true
    """
    try:
        # 优先读取 BI 专用环境变量，避免影响系统库
        db_url = db_url or os.getenv('BI_DB_URL')
        table_name = table_name or os.getenv('BI_TABLE_NAME') or 'bi_excel_processed'
        schema = schema or os.getenv('BI_SCHEMA')
        if_exists = os.getenv('BI_IF_EXISTS', if_exists) or if_exists
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

        # 严格分离：仅使用 BI_* 环境变量，避免误写入系统库

        if not db_url:
            raise ValueError("缺少数据库连接字符串: 请传入 db_url 或设置环境变量 BI_DB_URL")
        if not table_name:
            raise ValueError("缺少目标表名: 请传入 table_name 或设置环境变量 BI_TABLE_NAME")

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
                    pass
            # 读取 CSV 映射
            _alias_path = os.getenv('BI_ORG_ALIASES_PATH')
            if _alias_path and os.path.exists(_alias_path):
                try:
                    _csv_df = pd.read_csv(_alias_path)
                    if {'alias', 'canonical'}.issubset(set(_csv_df.columns)):
                        alias_map.update({str(r['alias']): str(r['canonical']) for _, r in _csv_df.iterrows()})
                except Exception:
                    pass

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
                    pass
        except Exception as e:
            import traceback
            traceback.print_exc()
        strict_only_append = (os.getenv('BI_STRICT_SCHEMA', 'false').lower() in ('1', 'true', 'yes', 'y'))
        no_ddl = strict_only_append or (os.getenv('BI_NO_DDL', 'false').lower() in ('1', 'true', 'yes', 'y'))
        if upsert_create_unique is None:
            upsert_create_unique = (os.getenv('BI_UPSERT_CREATE_UNIQUE', 'true').lower() in ('1', 'true', 'yes', 'y'))
        if no_ddl:
            upsert_create_unique = False
        # 内存层面去重：优先按 dedup_keys，否则按整行；
        # 保留首次出现，但时间列（表格日期_source/表格日期）按组保留最后值
        try:
            _before_cnt = len(df)
            time_cols = [c for c in ['表格日期_source', '表格日期'] if c in df.columns]
            if dedup_keys is None:
                if 'org_col' in locals() and 'time_col' in locals() and org_col and time_col:
                    dedup_keys = [org_col, time_col]
            if dedup_keys and all(k in df.columns for k in dedup_keys):
                # 检查DataFrame中的重复记录
                duplicate_records = df[df.duplicated(subset=dedup_keys, keep=False)]
                if not duplicate_records.empty:
                    # 可以选择保留第一条或最后一条记录
                    df = df.drop_duplicates(subset=dedup_keys, keep='last')
                
                # 保留首次的记录
                df_dedup = df.drop_duplicates(subset=dedup_keys, keep='first')
                # 时间列按组保留最后一次出现的值
                if time_cols:
                    # 修复：避免当time_cols中的列也是dedup_keys时出现的重复列名问题
                    # 首先确定哪些time_cols不在dedup_keys中，避免列名冲突
                    time_cols_for_grouping = [c for c in time_cols if c not in dedup_keys]
                    
                    if time_cols_for_grouping:
                        # 只对不在dedup_keys中的时间列进行处理
                        last_times = df.groupby(dedup_keys, sort=False)[time_cols_for_grouping].last().reset_index()
                        df_dedup = df_dedup.merge(last_times, on=dedup_keys, how='left', suffixes=('', '_last'))
                        for c in time_cols_for_grouping:
                            cl = c + '_last'
                            if cl in df_dedup.columns:
                                df_dedup[c] = df_dedup[cl]
                        df_dedup.drop(columns=[c + '_last' for c in time_cols_for_grouping if (c + '_last') in df_dedup.columns], inplace=True)
                    else:
                        # 如果所有time_cols都在dedup_keys中，则不需要额外处理时间列
                        # 因为它们已经在df_dedup中了
                        pass
                df = df_dedup
            else:
                # 无明确去重键时，按整行保留首次
                df = df.drop_duplicates(keep='first')
        except Exception as e:
            import traceback
            traceback.print_exc()
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

            if not no_ddl:
                if missing_cols:
                    quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    with engine.begin() as conn:
                        for col in missing_cols:
                            pg_type = _infer_pg_type(df[col])
                            ddl = f'ALTER TABLE {quoted_table} ADD COLUMN "{col}" {pg_type};'
                            conn.exec_driver_sql(ddl)
        except Exception as e:
            import traceback
            traceback.print_exc()
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
                    pass
                else:
                    quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    cols_sql = ', '.join([f'"{k}"' for k in dedup_keys])
                    sql = f'SELECT {cols_sql} FROM {quoted_table}'
                    try:
                        existing_df = pd.read_sql_query(sql, con=engine)
                        existing_set = set(map(tuple, existing_df[dedup_keys].values.tolist()))
                        _before_cnt = len(df)
                        # 显示将要被数据库去重过滤掉的记录
                        df_tuples = df[dedup_keys].apply(tuple, axis=1)
                        df = df[~df_tuples.isin(existing_set)]
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
        except Exception as e:
            import traceback
            traceback.print_exc()
        # 列位置保障（PostgreSQL）：保留原数据并调整列顺序
        # 方案：为目标列创建末尾临时列，复制数据 -> 删除旧列 -> 将临时列重命名为原列名
        # 好处：不丢失已有数据，且列位置移动到末尾，最终顺序固定为 [.., 地市, 表格日期_source, 表格日期]
        try:
            if not no_ddl and db_url and db_url.startswith('postgresql'):
                insp = inspect(engine)
                quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                try:
                    _cols = insp.get_columns(table_name, schema=schema)
                except Exception:
                    _cols = []
                _existing_names = {c['name'] for c in _cols}
                def _rebuild_preserve(col_name: str):
                    tmp_name = f"__tmp__{col_name}"
                    try:
                        with engine.begin() as conn:
                            # 清理可能存在的临时列
                            conn.exec_driver_sql(f'ALTER TABLE {quoted_table} DROP COLUMN IF EXISTS "{tmp_name}";')
                            # 在末尾创建临时列
                            conn.exec_driver_sql(f'ALTER TABLE {quoted_table} ADD COLUMN "{tmp_name}" TEXT;')
                            # 若原列存在，复制数据并删除原列
                            if col_name in _existing_names:
                                conn.exec_driver_sql(f'UPDATE {quoted_table} SET "{tmp_name}" = "{col_name}";')
                                conn.exec_driver_sql(f'ALTER TABLE {quoted_table} DROP COLUMN "{col_name}";')
                            # 临时列重命名为原列名（位置保留在末尾）
                            conn.exec_driver_sql(f'ALTER TABLE {quoted_table} RENAME COLUMN "{tmp_name}" TO "{col_name}";')
                    except Exception as e:
                        pass

                # 处理“地市”列（若当前 DataFrame 包含该列）
                if '地市' in df.columns:
                    _rebuild_preserve('地市')
                # 处理时间列（若当前 DataFrame 包含这些列）
                for _tcol in [c for c in ['表格日期_source', '表格日期'] if c in df.columns]:
                    _rebuild_preserve(_tcol)
        except Exception as e:
            import traceback
            traceback.print_exc()
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
                        # 与上方标准化保持一致：优先使用 "表格日期"，其次 "date_m"
                        _time_candidates = [_time_env, '表格日期', 'date_m']
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
                    insp2 = inspect(engine)
                    existing_cols2 = set()
                    try:
                        existing_cols2 = {c['name'] for c in insp2.get_columns(table_name, schema=schema)}
                    except Exception:
                        existing_cols2 = set()
                    if strict_only_append:
                        key_cols = [upsert_keys[0]] + ([upsert_keys[1]] if len(upsert_keys) > 1 else [])
                        if not set(key_cols).issubset(existing_cols2):
                            return False
                        effective_cols = [c for c in df.columns if c in existing_cols2]
                        df = df[effective_cols]
                        non_key_cols = [c for c in effective_cols if c not in key_cols]
                    else:
                        key_cols = upsert_keys
                        non_key_cols = [c for c in df.columns if c not in key_cols]
                    quoted_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    cols_sql = ', '.join([f'"{c}"' for c in df.columns])
                    vals_sql = ', '.join([f'%({c})s' for c in df.columns])
                    keys_sql = ', '.join([f'"{k}"' for k in key_cols])
                    update_sql = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols]) or ''
                    sql = f'INSERT INTO {quoted_table} ({cols_sql}) VALUES ({vals_sql}) ON CONFLICT ({keys_sql}) DO UPDATE SET {update_sql};'
                    if upsert_create_unique and key_cols:
                        idx_name = f"uidx_{table_name}_{'_'.join([str(k) for k in key_cols])}".replace(' ', '_').lower()
                        create_idx_sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON {quoted_table} ({keys_sql});'
                        try:
                            with engine.begin() as conn:
                                conn.exec_driver_sql(create_idx_sql)
                        except Exception as e:
                            pass
                    with engine.begin() as conn:
                        for i, rec in enumerate(df.to_dict(orient='records')):
                            try:
                                conn.exec_driver_sql(sql, rec)
                            except Exception as e:
                                # 检查是否是重复键错误
                                if 'already exists' in str(e):
                                    # 查询数据库中是否已存在该记录
                                    try:
                                        check_sql = f"SELECT 1 FROM {quoted_table} WHERE "
                                        conditions = []
                                        values = []
                                        for key in key_cols:
                                            conditions.append(f'"{key}" = %s')
                                            values.append(rec.get(key))
                                        check_sql += " AND ".join(conditions)
                                        result = conn.execute(sa.text(check_sql), values).fetchone()
                                    except Exception as check_e:
                                        pass
                                # 添加详细的错误信息
                                import traceback
                                traceback.print_exc()
                    return True
                else:
                    pass
            else:
                if upsert_enabled:
                    pass
        except Exception as e:
            import traceback
            traceback.print_exc()
        # 退回常规写入
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists=if_exists, index=False)
        return True
    except Exception as e:
        import traceback
        traceback.print_exc()
        return False


def run_bi_excel_batch(base: str, file_ids: list[str], params: Optional[Dict] = None) -> Dict:
    """
    批量下载 API Excel 并入库数据库（同步执行）。

    - base: 接口基础 URL（例如 http://host:port/download_excel）
    - file_ids: 机构/文件ID 列表，将作为参数 p_org_id 传入
    - params: 额外参数字典，默认包含 p_unit=1；若未提供 date_m 则取当天

    返回执行统计：{"successes":n, "failures":m, "total":t}
    """
    if not base:
        raise ValueError("缺少下载接口前缀: 请设置 base")
    base = base.rstrip('/')
    params = dict(params or {})
    params.setdefault("p_unit", 1)
    params.setdefault("date_m", datetime.date.today().strftime("%Y-%m-%d"))
    successes = 0
    failures = 0
    if not file_ids:
        return {"successes": successes, "failures": failures, "total": 0}
    table_mapping = {}
    _tbl_map = os.getenv('BI_TABLE_MAPPING_JSON')
    if _tbl_map:
        try:
            table_mapping = json.loads(_tbl_map)
        except Exception:
            table_mapping = {}
    for fid in file_ids:
        url = base
        try:
            per_params = dict(params or {})
            per_params["p_org_id"] = str(fid)
            excel_bytes = download_excel_to_bytes(url=url, params=per_params)
            if not excel_bytes:
                failures += 1
                continue
            df = process_excel_bytes(excel_bytes)
            if df is None:
                failures += 1
                continue
            excel_name = ''
            try:
                processor = ExcelHeaderProcessor(separator="_")
                file_like = BytesIO(excel_bytes)
                excel_name, _ = processor.get_name_time_filelike(file_like, sheet_name=0)
            except Exception:
                excel_name = ''
            try:
                df = enrich_city_column(df)
            except Exception as e:
                pass
            ok = insert_df_to_db(
                df=df,
                db_url=os.getenv('BI_DB_URL'),
                table_name=table_mapping.get(excel_name, os.getenv('BI_TABLE_NAME')),
                schema=os.getenv('BI_SCHEMA'),
                if_exists=os.getenv('BI_IF_EXISTS', 'append')
            )
            if ok:
                successes += 1
            else:
                failures += 1
        except Exception as e:
            failures += 1
    return {"successes": successes, "failures": failures, "total": len(file_ids)}


if __name__ == "__main__":
    # 环境变量驱动的示例运行，便于本地测试
    params = {
        "date_m": os.getenv("JOB_DATE_M") or os.getenv("BI_DATE_M") or datetime.date.today().strftime("%Y-%m-%d"),
        "p_unit": 1,
    }
    ids_env = os.getenv('JOB_FILE_IDS') or os.getenv('BI_FILE_IDS') or ''
    file_ids: list[str] = []
    if ids_env:
        try:
            parsed = json.loads(ids_env)
            if isinstance(parsed, list):
                file_ids = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            file_ids = [x.strip() for x in ids_env.split(',') if x.strip()]
    base = os.getenv('JOB_URL_BASE') or os.getenv('JOB_URL')
    if not base:
        raise ValueError("缺少下载接口前缀: 请设置 JOB_URL_BASE 或 JOB_URL")
    run_bi_excel_batch(base=base, file_ids=file_ids, params=params)