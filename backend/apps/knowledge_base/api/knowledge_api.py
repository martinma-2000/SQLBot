from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import asyncio

from apps.knowledge_base.ragflow_client import RAGFlowClient
from apps.knowledge_base.ragflow_retrieval_demo import KnowledgeRetriever

router = APIRouter()

# RAG配置
ragflow_base_url = os.getenv("RAGFLOW_BASE_URL", "http://localhost:180")
ragflow_api_key = os.getenv("RAGFLOW_API_KEY", "ragflow-U3ZDQwZTRlYjNhNDExZjA5MzkwNmUzNT")


class KnowledgeQueryRequest(BaseModel):
    question: str
    kb_name: Optional[str] = None


class KnowledgeQueryResponse(BaseModel):
    question: str
    llm_response: Dict[str, Any]
    parsed_knowledge: List[Dict[str, Any]]
    organization_info: List[Dict[str, Any]]


@router.post("/knowledge/query", response_model=KnowledgeQueryResponse)
async def query_knowledge_base(request: KnowledgeQueryRequest):
    """
    查询知识库并返回大模型处理结果和预处理后的JSON列表
    
    Args:
        request: 包含问题和知识库名称的请求对象
        
    Returns:
        包含大模型返回的JSON格式和知识库召回分片预处理后的JSON列表
    """
    try:
        # 初始化客户端和召回器
        client = RAGFlowClient(ragflow_base_url, ragflow_api_key)
        retriever = KnowledgeRetriever(client)
        
        # 获取知识库列表
        kbs = await client.get_knowledge_bases()
        if not kbs:
            raise HTTPException(status_code=404, detail="No knowledge bases found")
        
        # 确定要使用的知识库
        if request.kb_name:
            target_kb = next((kb for kb in kbs if kb.get('name') == request.kb_name), None)
            if not target_kb:
                raise HTTPException(status_code=404, detail=f"Knowledge base '{request.kb_name}' not found")
        else:
            # 使用第一个知识库
            target_kb = kbs[0]
        
        kb_id = target_kb.get('id')
        
        # 使用大模型分析问题
        llm_response = await client.analyze_question_with_llm(request.question)
        
        # 获取机构信息
        org_names = llm_response.get("机构信息", [])
        organization_info = await client.get_organization_info(org_names)
        
        # 召回相关分片
        chunks = await client.search_knowledge_base(kb_id, request.question, top_k=RAGFlowClient.DEFAULT_TOP_K)
        
        # 显示原始召回内容
        contents = [chunk.get('content', '') for chunk in chunks if chunk.get('content')]
        
        # 使用parse_to_json进行预处理
        parsed_data = client.parse_to_json(contents)
        
        return KnowledgeQueryResponse(
            question=request.question,
            llm_response=llm_response,
            parsed_knowledge=parsed_data,
            organization_info=organization_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")