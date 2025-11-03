import requests
import os
import pandas as pd
from excel_processing.excel_extract import ExcelHeaderProcessor

def download_excel_from_url(date_m,p_date_m,url,file_name):
    """
    从指定URL下载Excel文件,并保存在当前目录
    date_m:每月最后一天 --2025-03-31
    p_date_m:维度只到月份 --202503
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        with open(file_name, 'wb') as file:
            file.write(response.content)
        print(f"已下载文件: {file_name}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"下载文件时出错: {e}")
        return False

def process_excel_file(file_name):
    """
    使用ExcelHeaderProcessor处理下载的Excel文件
    """
    try:
        # 创建处理器实例
        processor = ExcelHeaderProcessor(separator="_")
        
        # 处理Excel文件，将多级表头转换为单级表头
        df = processor.convert_multi_to_single_header(file_name, header_rows=None)
        
        # 获取原始文件名（不含扩展名）
        base_name = os.path.splitext(file_name)[0]
        
        # 保存处理后的文件
        processed_filename = f"{base_name}_processed.xlsx"
        df.to_excel(processed_filename, index=False)
        print(f"已处理文件并保存为: {processed_filename}")
        
        # 删除原始文件，只保留处理后的文件
        try:
            os.remove(file_name)
            print(f"已删除原始文件: {file_name}")
        except Exception as e:
            print(f"删除原始文件时出错: {e}")
        
        return processed_filename
    except Exception as e:
        print(f"处理Excel文件时出错: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    date_m = "2025-03-31"
    p_date_m = "202503"
    # 真实url需要date_m和p_date_m作为日期参数，测试不需要
    url = 'http://127.0.0.1:8011/download_excel'
    file_name = f"{p_date_m}.xlsx"
    
    # 下载文件
    if download_excel_from_url(date_m, p_date_m, url, file_name):
        # 如果下载成功，则处理文件
        processed_file = process_excel_file(file_name)
        if processed_file:
            print(f"文件处理完成: {processed_file}")
        else:
            print("文件处理失败")
    else:
        print("文件下载失败")