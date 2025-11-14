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

    # 移除了对主键列值的校验部分，只保留列名校验

    try:
        result = pd.concat(dataframes, axis=0, ignore_index = True)
        # 添加去重处理，基于所有列进行去重
        result = result.drop_duplicates()
    except Exception as e:
        raise ValueError(f"拼接过程中发生错误:{str(e)}")
    return result


if __name__ == "__main__":
    excel_file = r"D:\文档-陕农信\测试文件示例\27000099_202509_元_银行卡业务月度运营报表.xlsx"