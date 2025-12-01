#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    RAGFlow客户端实现知识库中的文档召回功能
"""

import asyncio
import sys
import json
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from apps.knowledge_base.ragflow_client import RAGFlowClient
from common.utils.logger import logger


class KnowledgeRetriever:
    """知识召回器"""
    
    def __init__(self, client):
        self.client = client
    
    async def retrieve_relevant_chunks(self, question: str, kb_name: str = None, top_k: int = RAGFlowClient.DEFAULT_TOP_K):
        """
        从知识库中召回与问题最相关的分片
        
        Args:
            question: 用户的问题
            kb_name: 知识库名称，如果为None则使用第一个可用的知识库
            top_k: 返回最相关的分片数量
            
        Returns:
            相关分片列表
        """
        # 获取知识库列表
        kbs = await self.client.get_knowledge_bases()
        if not kbs:
            raise ValueError("No knowledge bases found")
        
        # 确定要使用的知识库
        if kb_name:
            target_kb = next((kb for kb in kbs if kb.get('name') == kb_name), None)
            if not target_kb:
                raise ValueError(f"Knowledge base '{kb_name}' not found")
        else:
            # 使用第一个知识库
            target_kb = kbs[0]
        
        kb_id = target_kb.get('id')
        print(f"Using knowledge base: {target_kb.get('name')}")
        
        # 执行召回
        print(f"Retrieving relevant chunks for question: {question}")
        chunks = await self.client.search_knowledge_base(kb_id, question, top_k)
        
        return chunks
    
    async def format_chunks_for_llm(self, chunks: list) -> str:
        """
        将召回的分片格式化为适合LLM使用的上下文
        
        Args:
            chunks: 召回的分片列表
            
        Returns:
            格式化的上下文字符串
        """
        if not chunks:
            return "No relevant information found."
        
        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            # 知识库召回片段源
            content = chunk.get('content', '')
            similarity = chunk.get('vector_similarity', chunk.get('similarity', 'N/A'))
            context_parts.append(f"Reference {i} (Similarity: {similarity}):\n{content}")
        
        return "\n\n".join(context_parts)

    async def query_and_respond(self, question: str, kb_name: str = None):
        """
        查询知识库并返回大模型处理结果和预处理后的JSON列表
        
        Args:
            question: 用户提出的问题
            kb_name: 知识库名称，如果为None则使用第一个可用的知识库
            
        Returns:
            包含大模型返回的JSON格式和知识库召回分片预处理后的JSON列表
        """
        # 获取知识库列表
        kbs = await self.client.get_knowledge_bases()
        if not kbs:
            raise ValueError("No knowledge bases found")
        
        # 确定要使用的知识库
        if kb_name:
            target_kb = next((kb for kb in kbs if kb.get('name') == kb_name), None)
            if not target_kb:
                raise ValueError(f"Knowledge base '{kb_name}' not found")
        else:
            # 使用第一个知识库
            target_kb = kbs[0]
        
        kb_id = target_kb.get('id')
        
        # 召回相关分片
        chunks = await self.client.search_knowledge_base(kb_id, question, top_k=RAGFlowClient.DEFAULT_TOP_K)
        
        # 显示原始召回内容
        contents = [chunk.get('content', '') for chunk in chunks if chunk.get('content')]
        
        # 使用parse_to_json进行预处理
        parsed_data = self.client.parse_to_json(contents)
        
        # 这里可以调用大模型对召回内容进行进一步处理
        # 暂时返回预处理的数据
        return {
            "question": question,
            "llm_response": {},  # 实际应用中这里应该是大模型的响应
            "parsed_knowledge": parsed_data
        }


async def main():
    # RAG配置 - 请根据实际情况修改
    ragflow_base_url = os.getenv("RAGFLOW_BASE_URL", "http://localhost:180")
    ragflow_api_key = os.getenv("RAGFLOW_API_KEY", "ragflow-U3ZDQwZTRlYjNhNDExZjA5MzkwNmUzNT")
    question = sys.argv[1] if len(sys.argv) > 1 else " "
    
    client = RAGFlowClient(ragflow_base_url, ragflow_api_key)
    retriever = KnowledgeRetriever(client)
    
    # 测试知识库召回功能
    print(f"Question: {question}\n")
    
    try:
        # 获取知识库列表
        kbs = await client.get_knowledge_bases()
        if not kbs:
            print("No knowledge bases found")
            return
        
        # 使用第一个知识库或指定的知识库
        target_kb = kbs[0]
        kb_id = target_kb.get('id')
        kb_name = target_kb.get('name')
        logger.info(f"Using knowledge base: {kb_name} (ID: {kb_id})")
        # 召回相关分片
        chunks = await client.search_knowledge_base(kb_id, question, top_k=RAGFlowClient.DEFAULT_TOP_K)
        
        # 显示原始召回内容
        for i, chunk in enumerate(chunks, 1):
            content = chunk.get('content', '')
            # similarity = chunk.get('vector_similarity', chunk.get('similarity', 'N/A'))
            # print(f"\n--- 分片 {i} (相似度: {similarity}) ---")
            # 只打印内容的前200个字符以避免日志过于冗长
            logger.info(f"Chunk {i}: {content[:200]}{'...' if len(content) > 200 else ''}")
        
        # 提取内容文本用于解析
        contents = [chunk.get('content', '') for chunk in chunks if chunk.get('content')]
        
        # 使用parse_to_json进行预处理
        parsed_data = client.parse_to_json(contents)
        
        # 显示解析后的结果
        print("\nParsed Data:")
        print(json.dumps(parsed_data, ensure_ascii=False, indent=2))

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())