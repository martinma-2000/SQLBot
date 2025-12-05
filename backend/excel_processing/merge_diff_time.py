"""
用法：多张表的格式相同，但是时间不同，将多张表纵向合并
"""
import pandas as pd
from typing import List
import re
from excel_processing.excel_extract import ExcelHeaderProcessor


def _is_explanatory_text_row(row):
    """判断一行是否为说明性文字或非数据内容"""
    row_strs = row.astype(str).str.strip()
    explanatory_keywords = [
        '计算公式', '参见', '指标', '说明', '备注', '注释', '公式', '方法',
        '日均增量', '净增', '占比', '合计', '统计', '时点', '余额', '行长', '主任'
    ]
    for cell in row_strs:
        if any(keyword in cell for keyword in explanatory_keywords):
            return True

    if len(row_strs) == 1:
        if re.match(r'^\d+\.', row_strs[0]):
            return True
        if '=' in row_strs[0] and any(keyword in row_strs[0] for keyword in ['计算', '公式', '增量']):
            return True
        if (not row_strs[0].isdigit() and any(c.isdigit() for c in row_strs[0]) and any(c.isalpha() for c in row_strs[0])):
            return True
    return False


def _is_number_like(s):
    try:
        if s is None:
            return False
        if isinstance(s, (int, float)):
            return True
        s = str(s).strip()
        if s == "":
            return False
        cleaned = s.replace(',', '').replace('%', '').replace('，', '')
        float(cleaned)
        return True
    except:
        return False


def _is_data_row(row, min_numeric_ratio: float = 0.4):
    values = list(row.values)
    total = len(values)
    if total == 0:
        return False
    numeric_count = sum(1 for v in values if _is_number_like(v))
    if numeric_count == 0:
        return False
    return (numeric_count / total) >= min_numeric_ratio


def remove_tail_rows_df(df: pd.DataFrame) -> pd.DataFrame:
    """删除尾部无效行（空行、说明性文字等），保留至最后一条有效数据"""
    if len(df) <= 1:
        return df

    last_valid_index = len(df) - 1
    for i in range(len(df) - 1, -1, -1):
        row = df.iloc[i]
        if row.isnull().all() or (row.astype(str).str.strip() == '').all() or _is_explanatory_text_row(row):
            continue
        if not _is_data_row(row):
            continue
        last_valid_index = i
        break

    invalid_rows = []
    for i in range(last_valid_index + 1, len(df)):
        row = df.iloc[i]
        if row.isnull().all() or (row.astype(str).str.strip() == '').all():
            invalid_rows.append(i)
        elif _is_explanatory_text_row(row):
            invalid_rows.append(i)
        else:
            break

    if invalid_rows:
        df = df.iloc[:last_valid_index + 1]
    return df

def concatenate_dataframes(dataframes: List[pd.DataFrame], primary_key_col: int = 0) -> pd.DataFrame:
    if not dataframes:
        raise ValueError("输入的DataFrame列表不能为空")
    
    # 统一清理所有 DataFrame 的尾部无效行
    try:
        dataframes = [remove_tail_rows_df(df) for df in dataframes]
        # 处理包含"编码"的列，将其转换为字符串类型
        processor = ExcelHeaderProcessor()
        dataframes = [processor.convert_encoding_columns_to_str(df) for df in dataframes]
    except Exception as e:
        raise ValueError(f"清理尾部无效行时发生错误:{str(e)}")
    
    # 2.统一列名校验
    reference_columns = list(dataframes[0].columns)
    for i, df in enumerate(dataframes[1:], 1):
        if list(df.columns) != reference_columns:
            raise ValueError(
                f"第{i + 1}个DataFrame的列名与第一个不一致\n"
                f"参考列名: {reference_columns}\n实际列名: {list(df.columns)}"
            )

    # 移除了对主键列值的校验部分，只保留列名校验

    try:
        result = pd.concat(dataframes, axis=0, ignore_index=True)
        # 添加去重处理，基于所有列进行去重
        result = result.drop_duplicates()
    except Exception as e:
        raise ValueError(f"纵向拼接过程中发生错误: {str(e)}")
    return result