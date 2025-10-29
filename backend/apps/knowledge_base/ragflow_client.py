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
    
    # 默认返回结果数量
    DEFAULT_TOP_K = 1
    
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
    
    async def analyze_question_with_llm(self, question: str) -> Dict[str, Any]:
        """
        使用大模型分析问题，提取机构信息和指标名称
        
        Args:
            question: 原始问题
            
        Returns:
            包含机构信息和指标名称的字典
        """
        try:
            # 获取默认的大模型配置
            config = await get_default_config()
            
            # 创建大模型实例
            llm_instance = LLMFactory.create_llm(config)
            llm = llm_instance.llm
            
            # 构造提示词
            prompt = f"""
            请从给定文本中识别并提取所有机构信息和相关指标名称，并以严格的JSON结构化形式输出。请务必遵守以下要求：

            输出格式：
            {{
            "机构信息": ["机构1", "机构2", ...],
            "指标名称": ["指标1", "指标2", ...]
            }}

            示例：
            输入：今年陕西农信的信用卡发卡量有多少？
            输出：
            {{
            "机构信息": ["陕西农信"],
            "指标名称": ["信用卡发卡量"]
            }}

            注意事项：
            1. 只提取文本中明确出现的机构名称和指标名称，不要推测或生成不存在的内容
            2. 输出必须为合法的JSON格式，无多余文字、标点或解释说明
            3. 不要输出除JSON结构外的任何文本或注释
            4. 字段名称必须严格为：机构信息、指标名称

            原始问题: {question}
            """
            
            # 调用大模型进行问题分析
            response = await llm.ainvoke(prompt)
            
            # 提取分析结果
            analysis_result = response.content.strip() if hasattr(response, 'content') else str(response).strip()
            
            # 尝试解析JSON
            try:
                parsed_result = json.loads(analysis_result)
                logger.info(f"问题分析成功: '{question}' -> {parsed_result}")
                return parsed_result
            except json.JSONDecodeError:
                logger.warning(f"大模型返回的不是有效JSON格式: {analysis_result}")
                # 返回默认结构
                return {"机构信息": [], "指标名称": []}
            
        except Exception as e:
            logger.error(f"大模型分析问题时出错: {e}")
            # 如果大模型分析失败，返回空的分析结果
            return {"机构信息": [], "指标名称": []}
    
    async def search_knowledge_base(self, kb_id: str, query: str, top_k: int = DEFAULT_TOP_K) -> List[Dict[str, Any]]:
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
            # 先使用大模型分析问题
            analysis_result = await self.analyze_question_with_llm(query)
            
            # 构建优化的查询语句
            query_parts = []
            if analysis_result.get("机构信息"):
                query_parts.extend(analysis_result["机构信息"])
            if analysis_result.get("指标名称"):
                query_parts.extend(analysis_result["指标名称"])
            
            # 如果提取到了关键信息，使用提取的信息作为查询，否则使用原始查询
            optimized_query = " ".join(query_parts) if query_parts else query
            
            payload = {
                "question": optimized_query,
                "dataset_ids": [kb_id],  # 根据API错误提示，使用 dataset_ids 而不是 datasets
                "top_k": top_k
            }
            
            logger.info(f"Sending request to RAGFlow API with top_k={top_k}")
            
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
                    
                    # 确保返回的分片数量不超过请求的top_k
                    if len(chunks) > top_k:
                        logger.info(f"RAGFlow returned {len(chunks)} chunks, truncating to top_k={top_k}")
                        chunks = chunks[:top_k]
                    
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
    
    def parse_to_json(self, contents: List[str]) -> List[Dict[str, Any]]:
        """
        将召回的内容列表解析为结构化的JSON格式
        
        Args:
            contents: 召回的内容字符串列表
            
        Returns:
            解析后的结构化数据列表
        """
        import re
        from typing import List, Dict, Any
        
        result = []
        
        for content in contents:
            if not isinstance(content, str) or not content.strip():
                continue
                
            try:
                # 提取指标编码 (idx_ecd/指标编号)
                idx_ecd_match = re.search(r'idx_ecd[/指标编号]*[：:]\s*([A-Z0-9_]+)', content)
                idx_ecd = idx_ecd_match.group(1).strip() if idx_ecd_match else ""
                
                # 提取指标名称
                indicator_match = re.search(r'指标名称[：:]\s*([^;；]+)', content)
                indicator_name = indicator_match.group(1).strip() if indicator_match else ""
                
                # 提取所有维度信息 - 匹配 dmns_cd1/维度1枚举值及含义:01：普卡，02：金卡... 的格式
                dimension_pattern = r'dmns_cd(\d+)/维度\d+枚举值及含义[：:]\s*([^;；]+)'
                dimension_matches = re.findall(dimension_pattern, content)
                
                dimensions = []
                for dim_num, dim_content in dimension_matches:
                    # 解析维度内的枚举值，格式如：01：普卡，02：金卡，03：白金卡，04：黑卡
                    enum_pattern = r'(\d+)[：:]\s*([^，,]+)'
                    enum_matches = re.findall(enum_pattern, dim_content)
                    
                    children = []
                    for enum_id, enum_label in enum_matches:
                        children.append({
                            "parentId": f"{idx_ecd}_dim{dim_num}",
                            "id": enum_id.strip(),
                            "label": enum_label.strip()
                        })
                    
                    # 创建维度节点
                    dimension = {
                        "parentId": idx_ecd,
                        "id": f"{idx_ecd}_dim{dim_num}",
                        "label": f"维度{dim_num}枚举值及含义",
                        "children": children
                    }
                    dimensions.append(dimension)
                
                # 构建根节点结果对象
                parsed_item = {
                    "parentId": "null",
                    "id": idx_ecd,
                    "label": indicator_name,
                    "children": dimensions
                }
                
                # 只有当至少有指标编码或指标名称时才添加到结果中
                if idx_ecd or indicator_name:
                    result.append(parsed_item)
                    
            except Exception as e:
                logger.warning(f"解析内容时出错: {e}, 内容: {content[:100]}...")
                continue
        
        return result
    
    async def search_and_parse_knowledge_base(self, kb_id: str, query: str, top_k: int = DEFAULT_TOP_K) -> List[Dict[str, Any]]:
        """
        在知识库中搜索相关内容并进行预处理
        
        Args:
            kb_id: 知识库ID
            query: 搜索查询
            top_k: 返回结果数量
            
        Returns:
            搜索并解析后的结构化数据列表
        """
        # 先获取原始召回数据
        chunks = await self.search_knowledge_base(kb_id, query, top_k)
        
        if not chunks:
            return []
        
        # 提取内容文本
        contents = []
        for chunk in chunks:
            content = chunk.get('content', '')
            if content:
                contents.append(content)
        
        # 解析为结构化数据
        parsed_data = self.parse_to_json(contents)
        
        logger.info(f"解析了 {len(contents)} 个召回片段，生成了 {len(parsed_data)} 个结构化数据项")
        
        return parsed_data
    
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
    
    async def main():
        # 注意：这里需要提供实际的 RAGFlow URL 和 API Key
        # 仅用于测试目的
        if len(sys.argv) < 3:
            print("Usage: python ragflow_client.py <base_url> <api_key> [question]")
            return
        # RAG配置
        base_url = sys.argv[1]
        api_key = sys.argv[2]
        question = sys.argv[3] if len(sys.argv) > 3 else "今年陕西农信的信用卡发卡量有多少？"
        
        client = RAGFlowClient(base_url, api_key)
        
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
            print(f"Using knowledge base: {kb_name} (ID: {kb_id})")
            
            print("\n" + "="*60)
            print("测试1: 原始召回数据")
            print("="*60)
            
            # 先使用大模型分析问题
            analysis_result = await client.analyze_question_with_llm(question)
            print(f"\n大模型解析结果: {json.dumps(analysis_result, ensure_ascii=False, indent=2)}")
            
            # 召回相关分片
            chunks = await client.search_knowledge_base(kb_id, question, top_k=RAGFlowClient.DEFAULT_TOP_K)
            print(f"召回了 {len(chunks)} 个分片")
            
            # 显示原始召回内容
            for i, chunk in enumerate(chunks, 1):
                content = chunk.get('content', '')
                similarity = chunk.get('vector_similarity', chunk.get('similarity', 'N/A'))
                print(f"\n--- 分片 {i} (相似度: {similarity}) ---")
                print(content)
            
            # 提取内容文本用于解析
            contents = [chunk.get('content', '') for chunk in chunks if chunk.get('content')]
            
            # 使用parse_to_json进行预处理
            parsed_data = client.parse_to_json(contents)
                
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
    
    # 运行异步主函数
    asyncio.run(main())