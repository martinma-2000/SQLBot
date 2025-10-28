"""
RAGFlow客户端模块
用于与RAGFlow知识库系统进行API交互
"""
import httpx
import json
from typing import Dict, List, Optional, Any
from common.utils.logger import logger

# 添加大模型相关的导入
from apps.ai_model.model_factory import LLMFactory, get_default_config


class RAGFlowClient:
    """RAGFlow API客户端"""
    
    def __init__(self, base_url: str, api_key: str):
        """
        初始化RAGFlow客户端
        
        Args:
            base_url: RAGFlow服务的基础URL
            api_key: API密钥
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    async def get_knowledge_bases(self) -> List[Dict[str, Any]]:
        """
        获取所有知识库列表
        
        Returns:
            知识库列表
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/datasets",
                    headers=self.headers,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    return data.get('data', [])
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return []
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when getting knowledge bases: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error when getting knowledge bases: {e}")
            return []
    
    async def get_knowledge_base_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        根据名称获取知识库信息
        
        Args:
            name: 知识库名称
            
        Returns:
            知识库信息，如果未找到返回None
        """
        knowledge_bases = await self.get_knowledge_bases()
        for kb in knowledge_bases:
            if kb.get('name') == name:
                return kb
        return None
    
    async def get_knowledge_base_documents(self, kb_id: str) -> List[Dict[str, Any]]:
        """
        获取知识库中的文档列表
        
        Args:
            kb_id: 知识库ID
            
        Returns:
            文档列表
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/datasets/{kb_id}/documents",
                    headers=self.headers,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    return data.get('data', [])
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return []
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when getting documents: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error when getting documents: {e}")
            return []
    
    async def get_document_detail(self, kb_id: str, doc_id: str) -> Dict[str, Any]:
        """
        获取文档详细信息
        
        Args:
            kb_id: 知识库ID
            doc_id: 文档ID
            
        Returns:
            文档详细信息
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/datasets/{kb_id}/documents/{doc_id}",
                    headers=self.headers,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    return data.get('data', {})
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return {}
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when getting document detail: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error when getting document detail: {e}")
            return {}
    
    async def get_document_chunks(self, kb_id: str, doc_id: str) -> List[Dict[str, Any]]:
        """
        获取文档中的分片列表
        
        Args:
            kb_id: 知识库ID
            doc_id: 文档ID
            
        Returns:
            分片列表
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/datasets/{kb_id}/documents/{doc_id}/chunks",
                    headers=self.headers,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    return data.get('data', [])
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return []
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when getting document chunks: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error when getting document chunks: {e}")
            return []
    
    async def analyze_question_with_llm(self, question: str) -> str:
        """
        使用大模型分析问题，优化查询语句
        
        Args:
            question: 原始问题
            
        Returns:
            优化后的问题
        """
        try:
            # 获取默认的大模型配置
            config = await get_default_config()
            
            # 创建大模型实例
            llm_instance = LLMFactory.create_llm(config)
            llm = llm_instance.llm
            
            # 构造提示词
            prompt = f"""
            你是一个智能问题优化助手。请分析以下用户问题，并对其进行优化，使其更适合在知识库中进行检索。
            请保持问题的核心语义不变，但可以：
            1. 补充必要的背景信息
            2. 重新组织语言结构
            3. 添加可能相关的关键词
            4. 消除歧义表达
            
            原始问题: {question}
            
            请直接输出优化后的问题，不要添加任何其他说明:
            """
            
            # 调用大模型进行问题分析
            response = await llm.ainvoke(prompt)
            
            # 提取优化后的问题
            optimized_question = response.content.strip() if hasattr(response, 'content') else str(response).strip()
            
            logger.info(f"问题优化: '{question}' -> '{optimized_question}'")
            
            return optimized_question
            
        except Exception as e:
            logger.error(f"大模型分析问题时出错: {e}")
            # 如果大模型分析失败，返回原始问题
            return question
    
    async def search_knowledge_base(self, kb_id: str, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        在知识库中搜索相关内容
        
        Args:
            kb_id: 知识库ID
            query: 搜索查询
            top_k: 返回结果数量
            
        Returns:
            搜索结果列表
        """
        try:
            # 先使用大模型分析和优化问题
            optimized_query = await self.analyze_question_with_llm(query)
            
            payload = {
                "question": optimized_query,
                "dataset_ids": [kb_id],  # 根据API错误提示，使用 dataset_ids 而不是 datasets
                "top_k": top_k
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/api/v1/retrieval",
                    headers=self.headers,
                    json=payload,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    # 提取chunks数据
                    result_data = data.get('data', {})
                    chunks = result_data.get('chunks', [])
                    return chunks
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return []
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when searching knowledge base: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error when searching knowledge base: {e}")
            return []
    
    async def get_knowledge_base_content(self, name: str) -> Dict[str, Any]:
        """
        获取指定名称知识库的完整内容信息
        
        Args:
            name: 知识库名称
            
        Returns:
            包含知识库信息和文档列表的字典
        """
        # 获取知识库信息
        kb_info = await self.get_knowledge_base_by_name(name)
        if not kb_info:
            return {
                'error': f'Knowledge base "{name}" not found',
                'knowledge_base': None,
                'documents': []
            }
        
        # 获取文档列表
        documents = await self.get_knowledge_base_documents(kb_info['id'])
        
        return {
            'knowledge_base': kb_info,
            'documents': documents,
            'total_documents': len(documents)
        }


if __name__ == "__main__":
    import asyncio
    import sys
    import json
    
    class KnowledgeRetriever:
        """知识召回器"""
        
        def __init__(self, client):
            self.client = client
        
        async def retrieve_relevant_chunks(self, question: str, kb_name: str = None, top_k: int = 5):
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
    
    async def main():
        # 注意：这里需要提供实际的 RAGFlow URL 和 API Key
        # 仅用于测试目的
        if len(sys.argv) < 3:
            print("Usage: python ragflow_client.py <base_url> <api_key> [question]")
            return
        # RAG配置
        base_url = sys.argv[1]
        api_key = sys.argv[2]
        question = sys.argv[3] if len(sys.argv) > 3 else "今年4月理财产品销售额"
        
        client = RAGFlowClient(base_url, api_key)
        
        # 测试知识库召回功能
        print(f"Question: {question}\n")
        
        try:
            # 创建召回器
            retriever = KnowledgeRetriever(client)
            
            # 召回相关分片
            chunks = await retriever.retrieve_relevant_chunks(question)
            
            # 格式化为LLM上下文
            context = await retriever.format_chunks_for_llm(chunks)
            print("Retrieved context for LLM:")
            print("-" * 50)
            print(context)
            print("-" * 50)
            
            # 显示详细信息
            print(f"\nRetrieved {len(chunks)} chunks:")
            for i, chunk in enumerate(chunks, 1):
                print(f"\n{i}. Similarity: {chunk.get('similarity', 'N/A')}")
                print(f"   Content: {chunk.get('content', '')[:100]}...")
                
        except Exception as e:
            print(f"Error: {e}")
    
    # 运行异步主函数
    asyncio.run(main())