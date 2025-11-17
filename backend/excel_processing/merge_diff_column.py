"""
用法：多张表的时间列相同，但其他列不同，将多张表横向合并
"""
import pandas as pd
from typing import List
from excel_processing.excel_extract import ExcelHeaderProcessor

def find_org_differences(reference_col, current_col):
    """
    找出当前列与参考列中所有不一致的行值

    参数：
        reference_col: 参考列
        current_col: 当前列

    返回：
        包含差异行的DtaFrame
    """

    # 创建临时 DataFrame 方便比较
    temp_df = pd.DataFrame({'reference': reference_col, 'current': current_col})

    # 找出不一致的行
    differences = temp_df[~temp_df['reference'].eq(temp_df['current'])]

    if differences.empty:
        print("√ 所有列值完全一致，没有发现任何差异")
        return None
    else:
        print("× 以下行存在差异：")
        print(differences)
        return differences


def merge_dataframes_horizontally(dataframes: List[pd.DataFrame], time_col: int = 0) -> pd.DataFrame:
    """
    横向拼接多个DataFrame，基于相同的时间列
    
    参数:
    - dataframes: DataFrame列表
    - time_col: 时间列的索引位置，默认为0（第一列）
    
    返回:
    - 合并后的DataFrame
    """
    if not dataframes:
        raise ValueError("输入的DataFrame列表不能为空")

    # 统一清理每个 DataFrame 的尾部无效行
    try:
        processor = ExcelHeaderProcessor(separator="_")
        dataframes = [processor.remove_tail_rows(df) for df in dataframes]
    except Exception as e:
        raise ValueError(f"清理尾部无效行时发生错误: {str(e)}")
    
    # 检查时间列索引是否有效
    if time_col >= len(dataframes[0].columns):
        raise ValueError(f"时间列索引{time_col}超出第一个DataFrame的列范围")
    
    # 获取参考时间列
    reference_time_col = dataframes[0].iloc[:, time_col]
    reference_time_col_name = dataframes[0].columns[time_col]
    
    # 获取参考机构列（假设为第一列）
    reference_org_col = dataframes[0].iloc[:, 0]
    reference_org_col_name = dataframes[0].columns[0]
    
    # 验证所有DataFrame
    for i, df in enumerate(dataframes[1:], 1):
        # 检查时间列索引是否有效
        if time_col >= len(df.columns):
            raise ValueError(f"时间列索引{time_col}超出第{i+1}个DataFrame的列范围")
        
        # 检查机构列名是否一致（机构列始终是第一列）
        if len(df.columns) <= 0 or df.columns[0] != reference_org_col_name:
            raise ValueError(f"第{i+1}个DataFrame的机构列名与第一个不一致\n"
                             f"参考列名: {reference_org_col_name}\n"
                             f"实际列名: {df.columns[0] if len(df.columns) > 0 else '无列'}")
        
        # 检查时间列名是否一致
        if df.columns[time_col] != reference_time_col_name:
            raise ValueError(f"第{i+1}个DataFrame的时间列名与第一个不一致\n"
                             f"参考列名: {reference_time_col_name}\n"
                             f"实际列名: {df.columns[time_col]}")
        
        # 检查机构列值是否一致
        if not df.iloc[:, 0].equals(reference_org_col):
            differences = find_org_differences(reference_org_col, df.iloc[:, 0])
            if differences is not None:
                print(differences)
            raise ValueError(f"第{i+1}个DataFrame的机构列值与第一个不一致\n"
                             f"请确保所有DataFrame的机构元素完全相同")
        
        # 检查时间列值是否一致
        if not df.iloc[:, time_col].equals(reference_time_col):
            raise ValueError(f"第{i+1}个DataFrame的时间列值与第一个不一致\n"
                             f"请确保所有DataFrame的第{time_col+1}列时间值完全相同")
        
        # 检查表格日期是否一致（这里假设表格日期在某固定位置，比如列名包含"表格日期"的列）
        # 查找包含"表格日期"的列
        date_cols_0 = [col for col in dataframes[0].columns if "表格日期" in str(col)]
        date_cols_i = [col for col in df.columns if "表格日期" in str(col)]
        
        # 确保都有表格日期列
        if len(date_cols_0) != len(date_cols_i):
            raise ValueError(f"第{i+1}个DataFrame与第一个DataFrame的表格日期列数不一致\n"
                             f"参考列数: {len(date_cols_0)}\n"
                             f"实际列数: {len(date_cols_i)}")
        
        # 检查表格日期列的内容是否一致
        for date_col in date_cols_0:
            if date_col in df.columns:
                if not dataframes[0][date_col].equals(df[date_col]):
                    raise ValueError(f"第{i+1}个DataFrame的表格日期列'{date_col}'与第一个DataFrame不一致\n"
                                     f"请确保所有DataFrame的表格日期完全相同")
            else:
                raise ValueError(f"第{i+1}个DataFrame缺少表格日期列'{date_col}'")
    
    try:
        # 以第一个DataFrame为基础，只保留时间列
        result = dataframes[0].iloc[:, [time_col]].copy()
        
        # 遍历所有DataFrame，将除时间列外的其他列添加到结果中
        for i, df in enumerate(dataframes):
            # 获取除时间列外的所有列
            other_columns = [col for j, col in enumerate(df.columns) if j != time_col]
            
            # 添加这些列到结果DataFrame中
            for col in other_columns:
                # 处理列名重复的情况，添加后缀
                new_col_name = col
                suffix = 1
                while new_col_name in result.columns:
                    new_col_name = f"{col}_{suffix}"
                    suffix += 1
                
                result[new_col_name] = df[col]
                
        # 添加去重处理，基于所有列进行去重
        result = result.drop_duplicates()
                
    except Exception as e:
        raise ValueError(f"横向拼接过程中发生错误: {str(e)}")
    
    return result


def merge_excel_files_horizontally(file_paths: List[str], time_col: int = 0, sheet_name=None) -> pd.DataFrame:
    """
    直接从Excel文件路径横向合并多个文件
    
    参数:
    - file_paths: Excel文件路径列表
    - time_col: 时间列的索引位置，默认为0（第一列）
    - sheet_name: 工作表名称，如果为None则读取第一个工作表
    
    返回:
    - 合并后的DataFrame
    """
    dataframes = []
    
    for file_path in file_paths:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        dataframes.append(df)
    
    return merge_dataframes_horizontally(dataframes, time_col)