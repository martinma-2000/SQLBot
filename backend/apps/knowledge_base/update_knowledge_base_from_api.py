"""
    功能：通过接口获取Excel更新知识库文档
"""
import asyncio
import httpx
import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from apps.knowledge_base.ragflow_client import RAGFlowClient
from common.utils.logger import logger


async def update_knowledge_base_from_api():
    """
    从API下载Excel文件并更新到知识库（自动选择第一个知识库和文档）
    """
    # 配置参数 - 请根据实际情况修改
    ragflow_base_url = os.getenv("RAGFLOW_BASE_URL", "http://localhost:180")  # RAGFlow服务地址
    ragflow_api_key = os.getenv("RAGFLOW_API_KEY", "ragflow-U3ZDQwZTRlYjNhNDExZjA5MzkwNmUzNT")       # RAGFlow API密钥
    
    # 源文件下载地址
    download_url = "http://localhost:8011/download_excel"
    
    # 创建RAGFlow客户端
    client = RAGFlowClient(ragflow_base_url, ragflow_api_key)
    
    try:
        # 1. 获取知识库列表
        logger.info("正在获取知识库列表...")
        kbs = await client.get_knowledge_bases()
        if not kbs:
            logger.error("未找到任何知识库")
            return False
        
        logger.info(f"找到 {len(kbs)} 个知识库")
        for i, kb in enumerate(kbs):
            logger.info(f"  {i+1}. {kb.get('name', 'Unknown')} (ID: {kb.get('id', 'Unknown')})")
        
        # 自动选择第一个知识库
        target_kb = kbs[0]
        kb_id = target_kb['id']
        kb_name = target_kb['name']
        logger.info(f"自动选择知识库: {kb_name} (ID: {kb_id})")
        
        # 2. 获取文档列表
        documents_response = await client.get_knowledge_base_documents(kb_id)
        
        # 检查响应格式并提取文档列表
        documents_list = []
        if isinstance(documents_response, list):
            documents_list = documents_response
        elif isinstance(documents_response, dict):
            # 检查常见的包含文档列表的字段
            found_docs = False
            for key in ['docs', 'data', 'documents', 'items', 'results']:
                if key in documents_response:
                    if isinstance(documents_response[key], list):
                        documents_list = documents_response[key]
                        found_docs = True
                        break
                    else:
                        # 即使不是列表，也可能是单个文档对象
                        documents_list = [documents_response[key]]
                        found_docs = True
                        break
            
            # 如果没有找到文档字段，将整个字典作为一个文档处理
            if not found_docs:
                documents_list = [documents_response]
        else:
            # 其他类型，转换为列表
            documents_list = [documents_response] if documents_response else []
            
        # 如果列表中的条目是包含docs字段的字典，则提取实际的文档列表
        if len(documents_list) == 1 and isinstance(documents_list[0], dict) and 'docs' in documents_list[0]:
            actual_documents = documents_list[0]['docs']
        else:
            actual_documents = documents_list
            
        if not actual_documents:
            logger.error("知识库中没有文档")
            return False
            
        # 自动选择第一个文档
        first_doc = actual_documents[0]
        if isinstance(first_doc, dict):
            doc_id = first_doc.get('id', '')
        else:
            doc_id = str(first_doc) if isinstance(first_doc, (str, int)) else ''
            
        # 检查是否成功获取了文档ID
        if not doc_id:
            logger.error("无法获取有效的文档ID")
            return False
        
        # 3. 从API下载Excel文件
        logger.info(f"正在从 {download_url} 下载Excel文件...")
        async with httpx.AsyncClient() as http_client:
            response = await http_client.get(download_url)
            response.raise_for_status()
            
            # 保存下载的文件
            file_path = "temp_download_file.xlsx"
            with open(file_path, "wb") as f:
                f.write(response.content)
            logger.info(f"文件已下载并保存为: {file_path}")
        
        # 4. 更新知识库文档
        logger.info("正在更新知识库中的文档...")
        success = await client.update_document(kb_id, doc_id, file_path)
        
        # 5. 清理下载的文件
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"临时文件 {file_path} 已清理")
        
        if success:
            logger.info("文档更新成功!")
            return True
        else:
            logger.error("文档更新失败!")
            return False
            
    except httpx.RequestError as e:
        logger.error(f"网络请求错误: {e}")
        # 确保清理临时文件
        if os.path.exists("temp_download_file.xlsx"):
            os.remove("temp_download_file.xlsx")
        return False
    except Exception as e:
        logger.error(f"更新知识库时出错: {e}", exc_info=True)
        # 确保清理临时文件
        if os.path.exists("temp_download_file.xlsx"):
            os.remove("temp_download_file.xlsx")
        return False


async def main():
    """
    主函数
    """
    logger.info("开始执行知识库更新任务...")
    logger.info(f"RAGFLOW_BASE_URL: {os.getenv('RAGFLOW_BASE_URL', 'http://localhost:180')}")
    logger.info(f"RAGFLOW_API_KEY: {os.getenv('RAGFLOW_API_KEY', 'your_api_key_here')}")
    
    success = await update_knowledge_base_from_api()
    if success:
        logger.info("知识库更新任务执行成功")
    else:
        logger.error("知识库更新任务执行失败")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())