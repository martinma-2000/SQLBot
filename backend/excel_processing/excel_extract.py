import pandas as pd
import re


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
        # 删除尾部无效行的改进逻辑
        df = self.remove_tail_rows(df)
        df['表格日期_source'] = self.parse_chinese_date(_time)

        def standardize_date_format(date_period):
            """将 Period 对象转换为标准日期格式"""
            if pd.isna(date_period):
                return None
            try:
                # 根据 Period 的频率返回不同的格式
                if date_period.freq.name in ['M', 'ME']:
                    # 月度数据，返回该月的最后一天，格式为 YYYY-MM-DD
                    return date_period.to_timestamp(how='end').date()
                elif date_period.freq.name in ['D', 'DE']:
                    # 每日数据，返回 YYYY-MM-DD 格式
                    return date_period.to_timestamp().date()
                else:
                    # 其他频率，转换为 timestamp
                    return date_period.to_timestamp().date()
            except:
                return None

        df['表格日期'] = df['表格日期_source'].apply(standardize_date_format)
        return df

    def remove_tail_rows(self, df):
        """
        改进的删除尾部无效行的方法
        删除尾部说明性文字/空行等非数据内容；以最后一条“数据行”为界
        """
        if len(df) <= 1:
            return df
            
        # 从底部开始查找，找到最后一个有效数据行
        last_valid_index = len(df) - 1
        
        # 从最后一行开始向上遍历
        for i in range(len(df) - 1, -1, -1):
            row = df.iloc[i]
            # 如果空行或说明性文字行，跳过
            if row.isnull().all() or (row.astype(str).str.strip() == '').all() or self._is_explanatory_text(row):
                continue
            # 非空但不是数据行（几乎没有数值），也跳过
            if not self._is_data_row(row):
                continue
            # 找到最后一个数据行
            last_valid_index = i
            break
        
        # 检查有效数据行之后的连续无效行
        # 包括空行和包含说明性文字的行
        invalid_rows = []
        for i in range(last_valid_index + 1, len(df)):
            row = df.iloc[i]
            # 检查该行是否为空行或包含说明性文字
            if row.isnull().all() or (row.astype(str).str.strip() == '').all():
                invalid_rows.append(i)
            elif self._is_explanatory_text(row):
                invalid_rows.append(i)
            else:
                # 遇到新的有效数据行，停止检查
                break
        
        # 如果存在无效行，则删除它们
        if invalid_rows:
            # 截取到last_valid_index这一行（包含）
            df = df.iloc[:last_valid_index + 1]
        
        return df

    def _is_explanatory_text(self, row):
        """
        判断一行是否包含说明性文字
        通常包含计算公式、指标说明等非数据内容
        """
        # 将行转换为字符串列表
        row_strs = row.astype(str).str.strip()
        
        # 常见的说明性文字关键词
        explanatory_keywords = [
            '计算公式', '参见', '指标', '说明', '备注', '注释', '公式', '方法',
            '日均增量', '净增', '占比', '合计', '统计', '时点', '余额', '行长', '主任'
        ]
        
        # 检查行中是否包含任何说明性关键词
        for cell in row_strs:
            if any(keyword in cell for keyword in explanatory_keywords):
                return True
        
        # 特殊情况：包含序号的行（如"1. 日均增量="）
        if len(row_strs) == 1:
            # 检查是否以数字+点号开头
            if re.match(r'^\d+\.', row_strs[0]):
                return True
            
            # 检查是否包含计算公式格式
            if '=' in row_strs[0] and any(keyword in row_strs[0] for keyword in ['计算', '公式', '增量']):
                return True
        
        # 如果行中只有一个单元格且包含数字和文本混合的内容，可能是说明性文字
        if len(row_strs) == 1 and not row_strs[0].isdigit():
            # 检查是否包含数字和文本混合的内容
            if any(c.isdigit() for c in row_strs[0]) and any(c.isalpha() for c in row_strs[0]):
                return True
        
        return False

    def _is_number_like(self, s):
        """判断字符串是否是数值样式（含逗号、百分号等）"""
        try:
            if s is None:
                return False
            if isinstance(s, (int, float)):
                return True
            s = str(s).strip()
            if s == "":
                return False
            # 移除常见修饰后尝试转换
            cleaned = s.replace(',', '').replace('%', '')
            # 处理中文千分位或空格
            cleaned = cleaned.replace('，', '')
            float(cleaned)
            return True
        except:
            return False

    def _is_data_row(self, row, min_numeric_ratio: float = 0.4):
        """根据数值占比判断是否为数据行"""
        values = list(row.values)
        total = len(values)
        if total == 0:
            return False
        numeric_count = 0
        for v in values:
            if self._is_number_like(v):
                numeric_count += 1
        # 若存在明显的数值（至少1个）且数值占比达到阈值，则认为是数据行
        if numeric_count == 0:
            return False
        return (numeric_count / total) >= min_numeric_ratio

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

