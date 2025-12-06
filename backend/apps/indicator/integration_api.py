# apps/indicator/integration_api.py
import json
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

IDX_COLUMNS = {
    "data_dt": "数据日期",
    "org_ecd": "机构编码",
    "idx_ecd": "指标编码",
    "dmns_cd1": "维度代码1",
    "dmns_cd2": "维度代码2",
    "dmns_cd3": "维度代码3",
    "dmns_cd4": "维度代码4",
    "dmns_cd5": "维度代码5",
    "idx_val": "指标值_当天",
    "idx_val_2d": "指标值_2天前",
    "idx_val_3d": "指标值_3天前",
    "idx_val_4d": "指标值_4天前",
    "idx_val_5d": "指标值_5天前",
    "idx_val_6d": "指标值_6天前",
    "idx_val_7d": "指标值_7天前",
    "idx_val_8d": "指标值_8天前",
    "idx_val_9d": "指标值_9天前",
    "idx_val_10d": "指标值_10天前",
    "idx_val_11d": "指标值_11天前",
    "idx_val_12d": "指标值_12天前",
    "idx_val_13d": "指标值_13天前",
    "idx_val_14d": "指标值_14天前",
    "idx_val_15d": "指标值_15天前",
    "idx_val_16d": "指标值_16天前",
    "idx_val_17d": "指标值_17天前",
    "idx_val_18d": "指标值_18天前",
    "idx_val_19d": "指标值_19天前",
    "idx_val_20d": "指标值_20天前",
    "idx_val_21d": "指标值_21天前",
    "idx_val_22d": "指标值_22天前",
    "idx_val_23d": "指标值_23天前",
    "idx_val_24d": "指标值_24天前",
    "idx_val_25d": "指标值_25天前",
    "idx_val_26d": "指标值_26天前",
    "idx_val_27d": "指标值_27天前",
    "idx_val_28d": "指标值_28天前",
    "idx_val_29d": "指标值_29天前",
    "idx_val_30d": "指标值_30天前",
    "idx_val_lyst": "指标值_上年同期",
    "idx_val_lsyed": "指标值_上年末",
    "idx_val_lmend": "指标值_上月末",
    "idx_val_l2mend": "指标值_上2月末",
    "idx_val_l3mend": "指标值_上3月末",
    "idx_val_l4mend": "指标值_上4月末",
    "idx_val_l5mend": "指标值_上5月末",
    "idx_val_l6mend": "指标值_上6月末",
    "idx_val_l7mend": "指标值_上7月末",
    "idx_val_l8mend": "指标值_上8月末",
    "idx_val_l9mend": "指标值_上9月末",
    "idx_val_l10mend": "指标值_上10月末",
    "idx_val_l11mend": "指标值_上11月末",
    "idx_val_l12mend": "指标值_上12月末"
}


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
    record_id: Optional[int] = None


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
        index_code = knowledge_response.parsed_knowledge[0].get("id") if knowledge_response.parsed_knowledge else "ELC_00000"

        # 执行指标管道，传入问题、组织代码和检索到的知识
        pipeline_result = execute_indicator_pipeline(request.question, org_code, rag_retrieved,index_code)
        
        # 保存执行结果到聊天记录中
        if pipeline_result.get("success"):
            # 保存SQL
            if pipeline_result.get("sql"):
                save_sql(session, record_id, pipeline_result["sql"])
            
            # 保存执行数据
            if pipeline_result.get("data"):

                # TODO:
                # 1. 将原始查询结果保存到 sql_exec_result 字段中
                # 2. 将每条数据的机构编码和指标编码映射成中文名
                # 3. 将处理后的结果保存到 data 字段中
                # sql_exec_data = pipeline_result["data"]
                # # 将 json 字符串转成python对象
                # sql_exec_data = json.loads(sql_exec_data)
                # for _data in sql_exec_data:
                #     # TODO:
                #     # 1) 将机构编码映射成机构名称，从数据库中查
                #     # 2) 将指标编码映射成指标名称，从入参中来
                #     pass

                import orjson
                save_sql_exec_data(session, record_id, orjson.dumps(pipeline_result["data"]).decode())
            
            # 保存图表信息
            if pipeline_result.get("parameters"):

                # TODO：
                # 1. 从 RESP 字段中解析出来查询的字段，也就是表头，然后映射成中文名
                # 2. 构造出参结构："chart": {"type": "", "title": "", "columns": [{"name":"机构名称", "value": "org_ecd"}]}
                # 3. 将结果存入 chart 中

                import orjson
                import json
                try:
                    # 尝试将parameters解析为JSON对象
                    params_dict = json.loads(pipeline_result["parameters"])
                    resp_fields = params_dict.get("RESP", [])
                    columns = []
                    for field in resp_fields:
                        field_name = IDX_COLUMNS.get(field, field)
                        columns.append({"name": field_name, "value": field})
                    chart_data = {
                        "type": "table",
                        "title": "",
                        "columns": columns
                    }
                except (json.JSONDecodeError, TypeError):
                    # 如果无法解析为JSON对象，则将其视为字符串
                    chart_data = {
                        "type": "table",
                        "title": "",
                        "columns": []
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
            error=pipeline_result.get("error"),
            record_id=record_id
        )
        
    except HTTPException:
        # 如果已有HTTP异常，重新抛出
        raise
    except Exception as e:
        # 如果在处理过程中出现异常，保存错误信息到聊天记录
        if chat_record:
            save_error_message(session, chat_record.id, str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")