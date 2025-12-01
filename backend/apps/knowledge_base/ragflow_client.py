"""
RAGFlow客户端模块
用于与RAGFlow知识库系统进行API交互
"""
import httpx
import json
import logging
import os
from typing import Dict, List, Optional, Any
from common.utils.logger import logger

# 添加大模型相关的导入
from apps.ai_model.model_factory import LLMFactory, get_default_config

# 添加数据库配置
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

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
        # RAGFlow数据库URL
        self.ragflow_db_url = os.getenv("RAGFLOW_DB_URL", "postgresql+psycopg://sqlbot_xh:11223344@localhost:45432/sqlbot_xh")
        # BI数据库URL
        self.bi_db_url = os.getenv("BI_DB_URL", "postgresql+psycopg://root:postgre%40123@localhost:35432/sqlbot_data")
    
    def get_database_engine(self):
        """
        获取数据库引擎
        
        Returns:
            SQLAlchemy引擎实例
        """
        engine = create_engine(self.ragflow_db_url)
        return engine
    
    def get_bi_database_engine(self):
        """
        获取BI数据库引擎
        
        Returns:
            SQLAlchemy引擎实例
        """
        engine = create_engine(self.bi_db_url)
        return engine
    
    def create_table_if_not_exists(self):
        """
        创建必要的表（如果不存在）
        """
        engine = self.get_database_engine()
        with engine.connect() as conn:
            # 创建用于存储查询历史的表
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_history (
                    id SERIAL PRIMARY KEY,
                    query_text TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    results JSONB
                )
            """))
            conn.commit()
        logger.info("确保query_history表存在")

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
                # 只在调试模式下记录详细日志
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"问题分析成功: '{question}' -> {parsed_result}")
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
                    
                    # 保存查询历史到数据库
                    try:
                        self.save_query_history(query, chunks)
                    except Exception as e:
                        logger.warning(f"保存查询历史时出错: {e}")
                    
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
    
    def save_query_history(self, query: str, results: List[Dict[str, Any]]):
        """
        保存查询历史到数据库
        
        Args:
            query: 查询文本
            results: 查询结果
        """
        engine = self.get_database_engine()
        try:
            with engine.connect() as conn:
                from datetime import datetime
                import json
                from sqlalchemy import text
                # 插入查询历史
                conn.execute(text("""
                    INSERT INTO query_history (query_text, results) 
                    VALUES (:query_text, :results)
                """), {
                    "query_text": query,
                    "results": json.dumps(results, ensure_ascii=False)
                })
                conn.commit()
            logger.info(f"查询历史已保存: {query}")
        except Exception as e:
            # 如果表不存在，则先创建表再尝试插入
            if "query_history" in str(e) and "does not exist" in str(e):
                logger.warning("查询历史表不存在，正在创建...")
                try:
                    self.create_table_if_not_exists()
                    # 重新尝试插入
                    with engine.connect() as conn:
                        from datetime import datetime
                        import json
                        from sqlalchemy import text
                        conn.execute(text("""
                            INSERT INTO query_history (query_text, results) 
                            VALUES (:query_text, :results)
                        """), {
                            "query_text": query,
                            "results": json.dumps(results, ensure_ascii=False)
                        })
                        conn.commit()
                    logger.info(f"查询历史表创建成功，查询历史已保存: {query}")
                except Exception as recreate_error:
                    logger.error(f"尝试创建表并保存查询历史时出错: {recreate_error}")
            else:
                logger.error(f"保存查询历史时出错: {e}")

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
    
    async def delete_document(self, kb_id: str, doc_id: str) -> bool:
        """
        删除知识库中的文档
        
        Args:
            kb_id: 知识库ID
            doc_id: 文档ID
            
        Returns:
            删除是否成功
        """
        try:
            async with httpx.AsyncClient() as client:
                # 使用通用request方法发送DELETE请求并附带请求体
                payload = {
                    "ids": [doc_id]
                }
                response = await client.request(
                    method="DELETE",
                    url=f"{self.base_url}/api/v1/datasets/{kb_id}/documents",
                    headers=self.headers,
                    json=payload,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    logger.info(f"Document {doc_id} deleted successfully from knowledge base {kb_id}")
                    return True
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return False
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when deleting document: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error when deleting document: {e}")
            return False
    
    async def upload_document(self, kb_id: str, file_path: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        上传文档到知识库
        
        Args:
            kb_id: 知识库ID
            file_path: 要上传的文件路径
            **kwargs: 其他参数，如parser_config等
            
        Returns:
            上传的文档信息，如果失败返回None
        """
        try:
            # 检查文件是否存在
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None
                
            # 准备multipart表单数据
            with open(file_path, 'rb') as file:
                files = {'file': (os.path.basename(file_path), file, 'application/octet-stream')}
                data = {'kb_id': kb_id}
                
                # 添加其他参数
                for key, value in kwargs.items():
                    data[key] = value
                
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"{self.base_url}/api/v1/datasets/{kb_id}/documents",
                        headers={'Authorization': self.headers['Authorization']},  # 不包含Content-Type
                        files=files,
                        data=data,
                        timeout=300.0  # 上传可能需要较长时间
                    )
                    
                    response.raise_for_status()
                    result = response.json()
                    
                    if result.get('code') == 0:
                        doc_info = result.get('data', {})
                        logger.info(f"Document uploaded successfully: {doc_info}")
                        
                        # 自动解析上传的文档
                        if isinstance(doc_info, dict) and 'id' in doc_info:
                            doc_ids = [doc_info['id']]
                        elif isinstance(doc_info, list) and len(doc_info) > 0 and 'id' in doc_info[0]:
                            doc_ids = [doc['id'] for doc in doc_info]
                        else:
                            doc_ids = []
                            
                        if doc_ids:
                            parse_success = await self.parse_documents(kb_id, doc_ids)
                            if parse_success:
                                logger.info(f"Document(s) {doc_ids} parsed successfully")
                            else:
                                logger.error(f"Failed to parse document(s) {doc_ids}")
                        
                        return doc_info
                    else:
                        logger.error(f"RAGFlow API error: {result.get('message', 'Unknown error')}")
                        return None
                        
        except httpx.RequestError as e:
            logger.error(f"Request error when uploading document: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error when uploading document: {e}")
            return None
    
    async def parse_documents(self, kb_id: str, document_ids: List[str]) -> bool:
        """
        解析知识库中的文档
        
        Args:
            kb_id: 知识库ID
            document_ids: 要解析的文档ID列表
            
        Returns:
            解析是否成功
        """
        try:
            async with httpx.AsyncClient() as client:
                payload = {
                    "document_ids": document_ids
                }
                response = await client.post(
                    f"{self.base_url}/api/v1/datasets/{kb_id}/chunks",
                    headers=self.headers,
                    json=payload,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                
                # 根据实际API响应格式处理数据
                if data.get('code') == 0:
                    logger.info(f"Documents {document_ids} parsed successfully in knowledge base {kb_id}")
                    return True
                else:
                    logger.error(f"RAGFlow API error: {data.get('message', 'Unknown error')}")
                    return False
                    
        except httpx.RequestError as e:
            logger.error(f"Request error when parsing documents: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error when parsing documents: {e}")
            return False
    
    async def update_document(self, kb_id: str, doc_id: str, new_file_path: str, **kwargs) -> bool:
        """
        更新知识库中的文档（先删除再上传）
        
        Args:
            kb_id: 知识库ID
            doc_id: 要更新的文档ID
            new_file_path: 新文件的路径
            **kwargs: 传递给上传方法的其他参数
            
        Returns:
            更新是否成功
        """
        try:
            # 1. 删除旧文档
            delete_success = await self.delete_document(kb_id, doc_id)
            if not delete_success:
                logger.error(f"Failed to delete old document {doc_id}")
                return False
                
            # 2. 上传新文档
            new_doc = await self.upload_document(kb_id, new_file_path, **kwargs)
            if not new_doc:
                logger.error(f"Failed to upload new document from {new_file_path}")
                return False
                
            logger.info(f"Document successfully updated. Old ID: {doc_id}, New ID: {new_doc.get('id') if isinstance(new_doc, dict) else new_doc[0].get('id') if isinstance(new_doc, list) and len(new_doc) > 0 else 'Unknown'}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating document: {e}")
            return False

    async def get_organization_info(self, org_names: List[str]) -> List[Dict[str, Any]]:
        """
        根据机构名称列表，在organization表中使用LIKE匹配branch_name和united_name字段，
        获取其branch_num或者united_num的数据（均优先获取branch_*）
        
        Args:
            org_names: 机构名称列表
            
        Returns:
            包含机构编号信息的字典列表
        """
        if not org_names:
            return []
            
        try:
            engine = self.get_bi_database_engine()
            results = []
            
            with engine.connect() as conn:
                for org_name in org_names:
                    # 使用LIKE进行模糊匹配，同时尝试多种匹配方式
                    query = text("""
                        SELECT branch_num, branch_name, united_num, united_name
                        FROM organization 
                        WHERE branch_name ILIKE :org_name1 
                           OR branch_short_name ILIKE :org_name1
                           OR united_name ILIKE :org_name1 
                           OR united_short_name ILIKE :org_name1
                           OR branch_name ILIKE :org_name2 
                           OR branch_short_name ILIKE :org_name2
                           OR united_name ILIKE :org_name2 
                           OR united_short_name ILIKE :org_name2
                        LIMIT 10
                    """)
                    
                    result = conn.execute(query, {
                        "org_name1": f"%{org_name}%",
                        "org_name2": f"{org_name}%"
                    })
                    rows = result.fetchall()
                    
                    for row in rows:
                        # 优先获取branch_num，如果没有则获取united_num
                        org_num = row[0] if row[0] is not None else row[2]
                        org_info = {
                            "org_name": org_name,
                            "matched_branch_name": row[1],
                            "matched_united_name": row[3],
                            "org_num": org_num
                        }
                        results.append(org_info)
                        
            return results
            
        except Exception as e:
            logger.error(f"查询机构信息时出错: {e}")
            return []