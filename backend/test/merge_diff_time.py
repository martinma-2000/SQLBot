"""
用法：多张表的格式相同，但是时间不同，将多张表纵向合并
"""
import pandas as pd
from typing import List
def concatenate_dataframes(dataframes: List[pd.DataFrame],primary_key_col: int = 0) ->pd.DataFrame:

    if not dataframes:
        raise ValueError("输入的DataFrame列表不能为空")
        # 2.统一列名校验
    reference_columns = list(dataframes[0].columns)
    for i, df in enumerate(dataframes[1:],1):
        if list(df.columns) != reference_columns:
            raise ValueError(f"第{i + 1}个Datarrame的列名每第一个不一致\n"
                             f"参考列名:{reference_columns}\n"
                             f"实际列名:{list(df.columns)}")

    if len(dataframes) > 1:
        # 获取所有主键值(去重后)
        primary_keys = set()
        for df in dataframes:
            # 检查主键列是否存在
            if primary_key_col >= len(df.columns):
                raise ValueError("主键列索引{primary key col}超出第{len(dataframes)}个DataFrame的列范围")

            current_keys = set(df.iloc[:, primary_key_col].astype(str))
            if not primary_keys:
                primary_keys = current_keys  # 否则检查是否一致
            elif current_keys != primary_keys:
                diff = primary_keys.symmetric_difference(current_keys)
                raise ValueError(f"主键值不一致，差异值:{diff}\n"
                                 f"请确保所有DataFrame的第{primary_key_col+1}列主键值完全相同")
    try:
        result = pd.concat(dataframes, axis=0, ignore_index = True)
    except Exception as e:
        raise ValueError(f"拼接过程中发生错误:{str(e)}")
    return result


if __name__ == "__main__":
    excel_file = r"D:\文档-陕农信\测试文件示例\27000099_202509_元_银行卡业务月度运营报表.xlsx"
