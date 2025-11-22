import os
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd
from sqlalchemy import create_engine


def _to_halfwidth(s: str) -> str:
    res = []
    for ch in s:
        code = ord(ch)
        if code == 12288:  # 全角空格
            code = 32
        elif 65281 <= code <= 65374:  # 全角字符范围
            code -= 65248
        res.append(chr(code))
    return ''.join(res)


def _normalize_org_name(s: Any) -> str:
    if s is None:
        return ''
    if not isinstance(s, str):
        s = str(s)
    s = _to_halfwidth(s)
    s = s.replace('（', '(').replace('）', ')')
    s = s.strip()
    s = ' '.join(s.split())
    return s


def _resolve_db_url(db_url: Optional[str]) -> str:
    return db_url or os.getenv('BI_DB_URL') or ''


def enrich_city_column(
    df: pd.DataFrame,
    org_column: Optional[str] = '机构名称',
    target_column: str = '地市',
    target_code_column: str = '编码',
    db_url: Optional[str] = None,
    schema: Optional[str] = None,
    org_table: str = 'organization',
    match_columns: Optional[List[str]] = None,
    city_column: str = 'city_short_name',
    code_columns: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    根据 BI 维度表 `organization` 为数据新增列 `地市` 和 `编码`：
    - 当 `branch_short_name`、`united_short_name` 或 `city_short_name` 任一等于 `机构名称` 时，
      取该行的 `city_short_name` 填充 `地市`，同时获取对应的编码（branch_num/united_num/city_num）。
    - 若无匹配则保持为空字符串。

    参数：
    - df: 原始 DataFrame（包含机构名称）
    - org_column: 机构名称列名，默认 '机构名称'。若该列不存在，将尝试 '机构'。
    - target_column: 新增（或覆盖）的地市列名，默认 '地市'
    - target_code_column: 新增（或覆盖）的编码列名，默认 '编码'
    - db_url: BI 数据库连接串，默认读取环境变量 BI_DB_URL
    - schema: schema 名，默认读取环境变量 BI_SCHEMA
    - org_table: 维度表名，默认 'organization'
    - match_columns: 用于匹配机构的列，默认 ['branch_short_name','united_short_name','city_short_name']
    - city_column: 地市列名，默认 'city_short_name'
    - code_columns: 匹配列到编码列的映射，默认 {
        'branch_short_name': 'branch_num',
        'united_short_name': 'united_num', 
        'city_short_name': 'city_num'
      }

    返回：
    - 填充后的 DataFrame（会就地复制并返回）
    """

    match_columns = match_columns or ['branch_short_name', 'united_short_name', 'city_short_name']
    code_columns = code_columns or {
        'branch_short_name': 'branch_num',
        'united_short_name': 'united_num',
        'city_short_name': 'city_num'
    }
    schema = schema or os.getenv('BI_SCHEMA')
    db_url_resolved = _resolve_db_url(db_url)
    if not db_url_resolved:
        raise ValueError('缺少数据库连接字符串: 请传入 db_url 或设置环境变量 BI_DB_URL')

    df = df.copy()

    # 选择机构列
    if org_column is None or org_column not in df.columns:
        candidates = [os.getenv('BI_ORG_COLUMN'), '机构名称', '机构']
        org_column = next((c for c in candidates if c and c in df.columns), None)
    if not org_column or org_column not in df.columns:
        # 无机构列时，直接创建空目标列
        df[target_column] = ''
        df[target_code_column] = ''
        print(f"! 未找到机构列，已创建空列 '{target_column}' 和 '{target_code_column}'")
        return df

    # 确保所有需要的列都在查询中
    all_required_columns = set(match_columns + [city_column])
    for match_col in match_columns:
        if match_col in code_columns:
            all_required_columns.add(code_columns[match_col])
    
    engine = create_engine(db_url_resolved)

    # 读取维度表（需要匹配列、城市列和编码列）
    quoted_table = f'"{schema}"."{org_table}"' if schema else f'"{org_table}"'
    select_cols = ', '.join([f'"{c}"' for c in all_required_columns])
    sql = f'SELECT {select_cols} FROM {quoted_table}'

    try:
        dim_df = pd.read_sql_query(sql, con=engine)
    except Exception as e:
        print(f"× 读取维度表失败: {e}")
        # 读取失败时，保留空列
        df[target_column] = ''
        df[target_code_column] = ''
        return df

    # 构建映射：任意匹配列的值 -> (city_short_name, 对应的编码)
    org_to_city_and_code: Dict[str, Tuple[str, str]] = {}
    for _, row in dim_df.iterrows():
        city_val = row.get(city_column)
        city_str = '' if pd.isna(city_val) else str(city_val)
        
        for mc in match_columns:
            val = row.get(mc)
            if pd.isna(val) or val is None:
                continue
            key = _normalize_org_name(val)
            # 保留首次映射，避免不同来源互相覆盖
            if key and key not in org_to_city_and_code:
                # 获取对应的编码
                code_col = code_columns.get(mc, '')
                code_val = row.get(code_col, '') if code_col else ''
                code_str = '' if pd.isna(code_val) else str(code_val)
                org_to_city_and_code[key] = (city_str, code_str)

    # 归一化机构名称并映射到城市和编码
    org_series = df[org_column].astype(str).map(_normalize_org_name)
    city_and_code_results = org_series.map(lambda k: org_to_city_and_code.get(k, ('', '')))
    
    # 分别提取地市和编码
    df[target_column] = city_and_code_results.apply(lambda x: x[0]).astype(str)
    df[target_code_column] = city_and_code_results.apply(lambda x: x[1]).astype(str)

    matched = (df[target_column] != '').sum()
    total = len(df)
    print(f"√ 地市列和编码列填充完成: 匹配 {matched}/{total}")
    return df


__all__ = ['enrich_city_column']