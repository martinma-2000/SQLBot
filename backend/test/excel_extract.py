import pandas as pd
import re
from test.merge_diff_time import concatenate_dataframes


class ExcelHeaderProcessor:
    """
    Excel多级表头处理器
    用于将多级表头的Excel文件转换为单级表头
    """

    def __init__(self, separator="_"):
        """
        初始化处理器
        
        参数:
        separator: 连接符，默认为下划线
        """
        self.separator = separator

    def get_name_time(self, file_path, sheet_name=0):
        """
        获取Excel文件的前两行数据,表名和时间

        参数:
        file_path: Excel文件路径
        sheet_name: 工作表名称或索引，默认为0

        返回:
        string: 表名,
        string: 表格时间
        """
        df = pd.read_excel(file_path, nrows=1, sheet_name=sheet_name)
        excel_name = [col for col in df.columns if not col.startswith('Unnamed')]
        excel_time = [data for data in df.values[0] if pd.notna(data) and '日期' in data]
        excel_time = excel_time[0].split('：')[-1]
        return excel_name[0],excel_time

    def detect_header_rows(self, file_path, sheet_name=0):
        """
        自动检测Excel文件中的表头行数
        假设第一行肯定是表头，从第二行开始检测是否为数据行
        通过检查第一列和第二列是否为非NaN值来判断数据行的开始位置
        
        参数:
        file_path: Excel文件路径
        sheet_name: 工作表名称或索引，默认为0（第一个工作表）
        
        返回:
        header_rows: 表头行数
        """
        # 读取前10行数据用于分析
        df_preview = pd.read_excel(file_path, nrows=10, header=None, sheet_name=sheet_name,skiprows=[0,1])
        # 至少有1行表头（第一行）
        header_rows = 1
        
        # 从第二行开始检查（索引为1）
        for index in range(1, len(df_preview)):
            row = df_preview.iloc[index]
            first_column_value = row.iloc[0]   # 第一列的值

            # 如果第一列不是NaN，说明这是数据行的开始
            if pd.notna(first_column_value):
                header_rows = index  # 表头行数就是数据行的索引（因为索引从0开始）
                break
        else:
            # 如果没有找到明确的数据行，最多认为前3行是表头
            header_rows = min(3, len(df_preview))
        
        return header_rows

    def _clean_column_name(self, name):
        """
        清理列名，去除"Unnamed"、"level"、纯数字等无用信息

        参数:
        name: 原始列名

        返回:
        cleaned_name: 清理后的列名
        """
        if not isinstance(name, str):
            name = str(name)

        # 去除"Unnamed"、"level"、纯数字等无用信息
        # 使用正则表达式匹配这些模式
        patterns_to_remove = [
            r'Unnamed:?\s*\d*',     # 匹配 "Unnamed" 或 "Unnamed: 0" 等
            r'level:?\s*\d*',       # 匹配 "level" 或 "level 0" 等
            r'^\d+$',               # 匹配纯数字
            r'^\s*$',               # 匹配纯空格或空字符串
        ]

        cleaned_name = name
        for pattern in patterns_to_remove:
            cleaned_name = re.sub(pattern, '', cleaned_name, flags=re.IGNORECASE)

        # 去除"_数字"格式的无效字段
        cleaned_name = re.sub(r'_\d+', '', cleaned_name)

        # 去除首尾空格
        cleaned_name = cleaned_name.strip()

        # 如果清理后为空，则返回None
        if not cleaned_name:
            return None

        return cleaned_name

    def _is_meaningful_part(self, part):
        """
        判断一个部分是否有意义（不是纯数字或特殊无意义字符）
        
        参数:
        part: 字符串部分
        
        返回:
        bool: 是否有意义
        """
        if not part or not isinstance(part, str):
            return False
        
        # 去除首尾空格
        part = part.strip()
        
        # 如果是空字符串，返回False
        if not part:
            return False
        
        # 如果是纯数字，返回False
        if part.isdigit():
            return False
        
        # 如果只包含下划线，返回False
        if re.match(r'^_+$', part):
            return False
        
        return True

    def convert_multi_to_single_header(self, file_path, header_rows=None, sheet_name=0):
        """
        将多级表头的Excel文件转换为单级表头
        
        参数:
        file_path: Excel文件路径
        header_rows: 表头行数，如果为None则自动检测
        sheet_name: 工作表名称或索引，默认为0（第一个工作表）
        
        返回:
        处理后的DataFrame
        """
        # 如果未指定表头行数，则自动检测
        if header_rows is None:
            header_rows = self.detect_header_rows(file_path, sheet_name)
            print(f"检测到表头行数: {header_rows}")
        
        # 如果检测到的表头行数为0，则至少保留1行作为表头
        if header_rows == 0:
            header_rows = 1
            #print("检测到表头行数为0，自动调整为1")
        adjusted_header_rows = [i + 2 for i in range(header_rows)]  # 跳过前两行
        # 读取Excel文件的表头部分
        header_df = pd.read_excel(file_path, header=adjusted_header_rows, sheet_name=sheet_name)
        # 获取列名（多级索引）
        multi_columns = header_df.columns

        # 将多级表头转换为单级表头
        single_columns = []
        for col in multi_columns:
            # 处理多级表头的每一级
            parts = []
            for level in col:
                if pd.notna(level):
                    # 清理每个层级的名称
                    cleaned_level = self._clean_column_name(level)
                    if cleaned_level:  # 只有清理后不为空才添加
                        parts.append(cleaned_level)
            
            # 过滤掉无意义的部分
            meaningful_parts = [part for part in parts if self._is_meaningful_part(part)]
            
            # 用分隔符连接各级表头
            if not meaningful_parts:
                # 如果所有部分都被清理掉了，则使用默认列名
                single_columns.append(f"column_{len(single_columns)}")
            else:
                # 连接各部分并确保没有多余的分隔符
                column_name = self.separator.join(meaningful_parts)
                # 清理可能的多余分隔符
                column_name = re.sub(f'{re.escape(self.separator)}+', self.separator, column_name)  # 合并多个连续分隔符
                column_name = column_name.strip(self.separator)  # 去除首尾分隔符
                
                # 再次清理 "_数字" 格式的无效字段
                # column_name = re.sub(r'_\d+', '', column_name)
                
                # 如果处理后仍然没有有意义的内容，则使用默认列名
                if not column_name or not self._is_meaningful_part(column_name):
                    single_columns.append(f"column_{len(single_columns)}")
                else:
                    single_columns.append(column_name)
        
        # 重新读取整个Excel文件
        df = pd.read_excel(file_path, header=list(range(header_rows)), sheet_name=sheet_name,skiprows=[0, 1])
        
        # 设置新的单级表头
        df.columns = single_columns

        excel_name, _time = self.get_name_time(file_path)
        if len(df) > 2:
            df = df.iloc[:-2]  # 删除最后两行
        df['表格日期'] = self.parse_chinese_date(_time)
        
        return df

    def parse_chinese_date(self, date_str):
        try:
            # 尝试按照 "YYYY年M月" 格式解析
            if '日' in date_str:
                # 处理年月日格式：YYYY年M月D日
                formatted_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
                return pd.Period(formatted_str, freq='D')
            else:
                # 处理年月格式：YYYY年M月
                formatted_str = date_str.replace('年', '-').replace('月', '')
                return pd.Period(formatted_str, freq='M')
        except Exception as e:
            print(f"无法解析日期字符串: {date_str}, 错误: {e}")
            return pd.NaT  # 返回 Not a Time 表示无效时间




# 使用示例
if __name__ == "__main__":
    excel_file = r"D:\文档-陕农信\测试文件示例\27000099_202509_银行卡发卡统计表6少机构.xlsx"
    
    try:
        # 创建处理器实例
        processor = ExcelHeaderProcessor(separator="_")

        excel_name,_time = processor.get_name_time(excel_file)

        # 处理Excel文件
        df = processor.convert_multi_to_single_header(excel_file, header_rows=None)
        if len(df) > 2:
            df = df.iloc[:-2]  # 删除最后两行

        df['表格日期'] = processor.parse_chinese_date(_time)


        df.to_excel(excel_name+"_单极表头.xlsx", index=False)
        print(excel_name+"_单极表头.xlsx")
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()

