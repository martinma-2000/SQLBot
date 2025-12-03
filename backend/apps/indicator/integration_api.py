# apps/indicator/integration_api.py

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any
import asyncio
import datetime

from apps.knowledge_base.api.knowledge_api import query_knowledge_base, KnowledgeQueryRequest
from apps.indicator.coordinator import execute_indicator_pipeline
from apps.chat.models.chat_model import Chat, ChatRecord
from apps.chat.curd.chat import create_chat, save_question, save_sql, save_sql_exec_data, save_chart, save_error_message, finish_record, get_chat_record_by_id
from common.core.deps import SessionDep, CurrentUser

router = APIRouter()


class IndicatorPipelineRequest(BaseModel):
    question: str
    kb_name: Optional[str] = None
    chat_id: Optional[int] = None


class IndicatorPipelineResponse(BaseModel):
    success: bool
    data: str
    sql: Optional[str]
    parameters: str
    knowledge_response: Optional[Dict[Any, Any]]
    error: Optional[str]


@router.post("/indicator/pipeline", response_model=IndicatorPipelineResponse)
async def run_indicator_pipeline(request: IndicatorPipelineRequest, session: SessionDep, current_user: CurrentUser):
    """
    运行指标管道：首先查询知识库获取相关信息，然后执行指标管道
    
    Args:
        request: 包含问题、知识库名称和可选的会话ID的请求对象
        session: 数据库会话
        current_user: 当前用户
        
    Returns:
        包含指标管道执行结果和知识库响应的综合结果
    """
    chat_record = None
    chat_id = None
    try:
        # 如果提供了chat_id，则使用现有会话，否则创建新的会话
        if request.chat_id:
            # 验证chat_id是否存在且属于当前用户
            chat = session.get(Chat, request.chat_id)
            if not chat:
                raise HTTPException(status_code=404, detail="Chat session not found")
            if chat.create_by != current_user.id:
                raise HTTPException(status_code=403, detail="Not authorized to access this chat session")
            chat_id = request.chat_id
        else:
            # 创建一个用于指标查询的聊天会话
            from apps.chat.models.chat_model import CreateChat
            create_chat_obj = CreateChat(
                question=request.question,
                origin=0  # 0: default, 1: mcp, 2: assistant
            )
            
            # 创建聊天会话（不需要数据源）
            chat_info = create_chat(session, current_user, create_chat_obj, require_datasource=False)
            chat_id = chat_info.id
        
        # 创建聊天记录
        from apps.chat.models.chat_model import ChatQuestion
        chat_question = ChatQuestion(
            chat_id=chat_id,
            question=request.question
        )
        
        chat_record = save_question(session, current_user, chat_question)
        record_id = chat_record.id
        
        # 首先查询知识库获取相关信息
        knowledge_request = KnowledgeQueryRequest(
            question=request.question,
            kb_name=request.kb_name
        )
        
        # 在事件循环中运行异步函数
        knowledge_response = await query_knowledge_base(knowledge_request)
        
        # 从知识库响应中提取有用信息作为上下文传递给指标管道
        # 根据execute_indicator_pipeline函数签名和返回值示例，我们需要提供user_query, org_code和rag_retrieved参数
        # org_code来自organization_info中的org_num
        # rag_retrieved来自parsed_knowledge
        org_code = knowledge_response.organization_info[0].get("org_num") if knowledge_response.organization_info else "000000"
        rag_retrieved = str(knowledge_response.parsed_knowledge)  # 将解析后的知识转换为字符串
        
        # 执行指标管道，传入问题、组织代码和检索到的知识
        pipeline_result = execute_indicator_pipeline(request.question, org_code, rag_retrieved)
        
        # 保存执行结果到聊天记录中
        if pipeline_result.get("success"):
            # 保存SQL
            if pipeline_result.get("sql"):
                save_sql(session, record_id, pipeline_result["sql"])
            
            # 保存执行数据
            if pipeline_result.get("data"):
                import orjson
                save_sql_exec_data(session, record_id, orjson.dumps(pipeline_result["data"]).decode())
            
            # 保存图表信息
            if pipeline_result.get("parameters"):
                import orjson
                import json
                try:
                    # 尝试将parameters解析为JSON对象
                    params_dict = json.loads(pipeline_result["parameters"])
                    chart_data = {
                        "type": "table",
                        "columns": [{"name": str(key), "value": str(key)} for key in params_dict.keys()]
                    }
                except (json.JSONDecodeError, TypeError):
                    # 如果无法解析为JSON对象，则将其视为字符串
                    chart_data = {
                        "type": "table",
                        "columns": [{"name": "parameters", "value": "parameters"}]
                    }
                save_chart(session, record_id, orjson.dumps(chart_data).decode())
        else:
            # 保存错误信息
            if pipeline_result.get("error"):
                save_error_message(session, record_id, pipeline_result["error"])
        
        # 标记记录完成
        finish_record(session, record_id)
        
        # 返回整合的结果
        return IndicatorPipelineResponse(
            success=pipeline_result["success"],
            data=pipeline_result.get("data"),
            sql=pipeline_result.get("sql"),
            parameters=pipeline_result.get("parameters"),
            knowledge_response=knowledge_response.dict() if knowledge_response else None,
            error=pipeline_result.get("error")
        )
        
    except HTTPException:
        # 如果已有HTTP异常，重新抛出
        raise
    except Exception as e:
        # 如果在处理过程中出现异常，保存错误信息到聊天记录
        if chat_record:
            save_error_message(session, chat_record.id, str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")