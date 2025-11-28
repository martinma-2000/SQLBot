from typing import Optional
import asyncio
import io
import traceback

import numpy as np
import orjson
import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy import and_, select
from pydantic import BaseModel

from apps.chat.curd.chat import list_chats, get_chat_with_records, create_chat, rename_chat, \
    delete_chat, get_chat_chart_data, get_chat_predict_data, get_chat_with_records_with_data, get_chat_record_by_id
from apps.chat.models.chat_model import CreateChat, ChatRecord, RenameChat, ChatQuestion, ExcelData
from apps.chat.task.llm import LLMService
from common.core.deps import CurrentAssistant, SessionDep, CurrentUser, Trans

router = APIRouter(tags=["Data Q&A"], prefix="/chat")

# 添加新的数据模型用于历史查询接口
class HistoryQueryResponse(BaseModel):
    limit: int
    has_more: bool
    data: list

@router.get("/list")
async def chats(session: SessionDep, current_user: CurrentUser):
    return list_chats(session, current_user)


# 添加新的历史查询接口
@router.get("/history-query")
async def history_query(session: SessionDep, current_user: CurrentUser, limit: int = 10):
    """
    获取历史会话列表，格式化为类似 Dify 的历史会话格式
    """
    # 获取聊天列表
    chat_list = list_chats(session, current_user)
    
    # 转换为 Dify 格式
    dify_format_data = []
    
    # 限制返回的聊天数量
    chats_to_process = chat_list[:limit]
    
    for chat in chats_to_process:
        # 为每个聊天创建一个类似 Dify 的记录
        dify_record = {
            "id": str(chat.id) if chat.id else "",
            "conversation_id": str(chat.id) if chat.id else "",
            "inputs": {
                "file": None
            },
            "query": chat.brief if chat.brief else "",
            "message": "",
            "message_tokens": 0,
            "answer": "",
            "answer_tokens": 0,
            "provider_response_latency": 0.0,
            "from_source": "api",
            "from_end_user_id": str(current_user.id) if current_user.id else "",
            "from_account_id": None,
            "feedbacks": [],
            "workflow_run_id": None,
            "annotation": None,
            "annotation_hit_history": None,
            "created_at": int(chat.create_time.timestamp()) if chat.create_time else 0,
            "agent_thoughts": [],
            "message_files": [],
            "metadata": {},
            "status": "completed",
            "error": None,
            "parent_message_id": None
        }
        
        # 如果聊天有错误信息，则更新状态和错误信息
        if hasattr(chat, 'error') and chat.error:
            dify_record["status"] = "error"
            dify_record["error"] = chat.error
        
        dify_format_data.append(dify_record)
    
    return {
        "limit": limit,
        "has_more": len(chat_list) > limit,
        "data": dify_format_data
    }


@router.get("/get/{chart_id}")
async def get_chat(session: SessionDep, current_user: CurrentUser, chart_id: int, current_assistant: CurrentAssistant):
    def inner():
        return get_chat_with_records(chart_id=chart_id, session=session, current_user=current_user,
                                     current_assistant=current_assistant)

    return await asyncio.to_thread(inner)


@router.get("/get/with_data/{chart_id}")
async def get_chat_with_data(session: SessionDep, current_user: CurrentUser, chart_id: int,
                             current_assistant: CurrentAssistant):
    def inner():
        return get_chat_with_records_with_data(chart_id=chart_id, session=session, current_user=current_user,
                                               current_assistant=current_assistant)

    return await asyncio.to_thread(inner)


@router.get("/record/get/{chart_record_id}/data")
async def chat_record_data(session: SessionDep, chart_record_id: int):
    def inner():
        return get_chat_chart_data(chart_record_id=chart_record_id, session=session)

    return await asyncio.to_thread(inner)


@router.get("/record/get/{chart_record_id}/predict_data")
async def chat_predict_data(session: SessionDep, chart_record_id: int):
    def inner():
        return get_chat_predict_data(chart_record_id=chart_record_id, session=session)

    return await asyncio.to_thread(inner)


@router.post("/rename")
async def rename(session: SessionDep, chat: RenameChat):
    try:
        return rename_chat(session=session, rename_object=chat)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.get("/delete/{chart_id}")
async def delete(session: SessionDep, chart_id: int):
    try:
        return delete_chat(session=session, chart_id=chart_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.post("/start")
async def start_chat(session: SessionDep, current_user: CurrentUser, create_chat_obj: CreateChat):
    try:
        return create_chat(session, current_user, create_chat_obj)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.post("/assistant/start")
async def start_chat(session: SessionDep, current_user: CurrentUser):
    try:
        return create_chat(session, current_user, CreateChat(origin=2), False)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.post("/recommend_questions/{chat_record_id}")
async def recommend_questions(session: SessionDep, current_user: CurrentUser, chat_record_id: int,
                              current_assistant: CurrentAssistant):
    def _return_empty():
        yield 'data:' + orjson.dumps({'content': '[]', 'type': 'recommended_question'}).decode() + '\n\n'

    try:
        record = get_chat_record_by_id(session, chat_record_id)

        if not record:
            return StreamingResponse(_return_empty(), media_type="text/event-stream")

        request_question = ChatQuestion(chat_id=record.chat_id, question=record.question if record.question else '')

        llm_service = await LLMService.create(current_user, request_question, current_assistant, True)
        llm_service.set_record(record)
        llm_service.run_recommend_questions_task_async()
    except Exception as e:
        traceback.print_exc()

        def _err(_e: Exception):
            yield 'data:' + orjson.dumps({'content': str(_e), 'type': 'error'}).decode() + '\n\n'

        return StreamingResponse(_err(e), media_type="text/event-stream")

    return StreamingResponse(llm_service.await_result(), media_type="text/event-stream")


@router.post("/question")
async def stream_sql(session: SessionDep, current_user: CurrentUser, request_question: ChatQuestion,
                     current_assistant: CurrentAssistant):
    """SQL analysis results

    Args:
        session: Database session
        current_user: CurrentUser
        request_question: User question model

    Returns:
        JSON response with analysis results
    """

    try:
        llm_service = await LLMService.create(current_user, request_question, current_assistant, embedding=True)
        llm_service.init_record()
        llm_service.run_task_async()
        
        # Collect all streaming results into a single response
        result_data = []
        for chunk in llm_service.await_result():
            result_data.append(chunk)
            
        # Process and combine the chunks into a final response
        final_result = {
            "content": "",
            "type": "finish"
        }
        
        # Extract content from chunks, handling the data: prefix format
        sql_content = ""
        chart_content = ""
        chart_data = None
        
        for chunk in result_data:
            if isinstance(chunk, str):
                # Handle string chunks that might contain data: prefix
                if chunk.startswith('data:'):
                    try:
                        # Extract JSON from data: prefix
                        json_str = chunk[5:]  # Remove 'data:' prefix
                        chunk_data = orjson.loads(json_str)
                        if chunk_data.get('content'):
                            content = chunk_data['content']
                            # Handle different content types
                            if chunk_data.get('type') == 'sql':
                                sql_content = content
                                final_result["sql"] = content
                            elif chunk_data.get('type') == 'chart':
                                try:
                                    chart_data = orjson.loads(content)
                                    final_result["chart"] = chart_data
                                except:
                                    chart_content = content
                            else:
                                final_result["content"] += content + "\n"
                        if chunk_data.get('type'):
                            final_result["type"] = chunk_data['type']
                        if chunk_data.get('data'):
                            final_result["data"] = chunk_data['data']
                    except:
                        # If parsing fails, add raw content
                        final_result["content"] += chunk + "\n"
                else:
                    final_result["content"] += chunk + "\n"
            elif isinstance(chunk, dict):
                if chunk.get('content'):
                    final_result["content"] += chunk['content'] + "\n"
                if chunk.get('type'):
                    final_result["type"] = chunk['type']
                if chunk.get('data'):
                    final_result["data"] = chunk['data']
                if chunk.get('sql'):
                    final_result["sql"] = chunk['sql']
                if chunk.get('chart'):
                    final_result["chart"] = chunk['chart']
                    
        # If we have specific content types, structure the response better
        if sql_content:
            final_result["content"] = sql_content
        elif chart_content:
            final_result["content"] = chart_content
            
        # If no content was collected, use the last chunk
        if not final_result["content"] and result_data:
            last_chunk = result_data[-1]
            if isinstance(last_chunk, dict):
                final_result.update(last_chunk)
            else:
                final_result["content"] = str(last_chunk)
                
        return JSONResponse(content=final_result)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            content={'content': str(e), 'type': 'error'},
            status_code=500
        )


@router.post("/execute-sql")
async def execute_sql(session: SessionDep, current_user: CurrentUser, request: dict,
                     current_assistant: CurrentAssistant):
    """Execute SQL directly and return results

    Args:
        session: Database session
        current_user: CurrentUser
        request: {sql: string, chat_id: int}

    Returns:
        JSON response with execution results
    """
    try:
        # Create ChatQuestion object with provided SQL
        user_sql = request.get('sql')
        chat_question = ChatQuestion(
            chat_id=request.get('chat_id'),
            question="Execute SQL directly：" + user_sql,
            sql=user_sql
        )

        llm_service = await LLMService.create(current_user, chat_question, current_assistant, embedding=False)
        llm_service.init_record()

        # Run task with direct SQL execution mode
        llm_service.execute_direct_sql_async()
        
        # Collect all streaming results into a single response
        result_data = []
        for chunk in llm_service.await_result():
            result_data.append(chunk)
            
        # Process and combine the chunks into a final response
        final_result = {
            "content": "",
            "type": "finish"
        }
        
        # Extract content from chunks, handling the data: prefix format
        has_error = False
        for chunk in result_data:
            if isinstance(chunk, str):
                # Handle string chunks that might contain data: prefix
                if chunk.startswith('data:'):
                    try:
                        # Extract JSON from data: prefix
                        json_str = chunk[5:]  # Remove 'data:' prefix
                        chunk_data = orjson.loads(json_str)
                        if chunk_data.get('content'):
                            # Check if this is an error message
                            content = chunk_data['content']
                            if '"type":"error"' in content or '"type":"exec-sql-err"' in content:
                                try:
                                    error_data = orjson.loads(content)
                                    if error_data.get('message'):
                                        final_result["content"] = error_data['message']
                                        final_result["type"] = "error"
                                        has_error = True
                                        break
                                except:
                                    final_result["content"] += content + "\n"
                            else:
                                final_result["content"] += content + "\n"
                        if chunk_data.get('type') and not has_error:
                            final_result["type"] = chunk_data['type']
                        if chunk_data.get('data'):
                            final_result["data"] = chunk_data['data']
                        if chunk_data.get('sql'):
                            final_result["sql"] = chunk_data['sql']
                    except:
                        # If parsing fails, add raw content
                        final_result["content"] += chunk + "\n"
                else:
                    final_result["content"] += chunk + "\n"
            elif isinstance(chunk, dict):
                if chunk.get('content'):
                    final_result["content"] += chunk['content'] + "\n"
                if chunk.get('type'):
                    final_result["type"] = chunk['type']
                if chunk.get('data'):
                    final_result["data"] = chunk['data']
                if chunk.get('sql'):
                    final_result["sql"] = chunk['sql']
                    
        # If no content was collected, use the last chunk
        if not final_result["content"] and result_data:
            last_chunk = result_data[-1]
            if isinstance(last_chunk, dict):
                final_result.update(last_chunk)
            else:
                final_result["content"] = str(last_chunk)
            
        return JSONResponse(content=final_result)

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            content={'content': str(e), 'type': 'error'},
            status_code=500
        )


from pydantic import BaseModel

class AnalysisOrPredictRequest(BaseModel):
    prompt: Optional[str] = None

@router.post("/record/{chat_record_id}/{action_type}")
async def analysis_or_predict(session: SessionDep, current_user: CurrentUser, chat_record_id: int, action_type: str,
                              current_assistant: CurrentAssistant, request_body: AnalysisOrPredictRequest = None):
    try:
        if action_type != 'analysis' and action_type != 'predict':
            raise Exception(f"Type {action_type} Not Found")
        record: ChatRecord | None = None

        stmt = select(ChatRecord.id, ChatRecord.question, ChatRecord.chat_id, ChatRecord.datasource,
                      ChatRecord.engine_type,
                      ChatRecord.ai_modal_id, ChatRecord.create_by, ChatRecord.chart, ChatRecord.data).where(
            and_(ChatRecord.id == chat_record_id))
        result = session.execute(stmt)
        for r in result:
            record = ChatRecord(id=r.id, question=r.question, chat_id=r.chat_id, datasource=r.datasource,
                                engine_type=r.engine_type, ai_modal_id=r.ai_modal_id, create_by=r.create_by,
                                chart=r.chart,
                                data=r.data)

        if not record:
            raise Exception(f"Chat record with id {chat_record_id} not found")

        if not record.chart:
            raise Exception(
                f"Chat record with id {chat_record_id} has not generated chart, do not support to analyze it")

        # 如果有提示词，将其添加到问题中
        prompt = request_body.prompt if request_body else None
        print(f"Prompt received: {prompt}")
        print(f"Record question: {record.question}")
        question = record.question
        if prompt:
            question = f"{question} {prompt}"
            print(f"Combined question: {question}")
        request_question = ChatQuestion(chat_id=record.chat_id, question=question)

        llm_service = await LLMService.create(current_user, request_question, current_assistant)
        llm_service.run_analysis_or_predict_task_async(action_type, record)
        
        # Collect all streaming results into a single response
        result_data = []
        for chunk in llm_service.await_result():
            result_data.append(chunk)
            
        # Process and combine the chunks into a final response
        final_result = {
            "content": "",
            "type": "finish"
        }
        
        # Extract content from chunks, handling the data: prefix format
        analysis_content = ""
        
        for chunk in result_data:
            if isinstance(chunk, str):
                # Handle string chunks that might contain data: prefix
                if chunk.startswith('data:'):
                    try:
                        # Extract JSON from data: prefix
                        json_str = chunk[5:]  # Remove 'data:' prefix
                        chunk_data = orjson.loads(json_str)
                        if chunk_data.get('content'):
                            content = chunk_data['content']
                            # Handle different content types
                            if chunk_data.get('type') == 'analysis-result':
                                analysis_content += content
                            else:
                                final_result["content"] += content + "\n"
                        if chunk_data.get('type'):
                            final_result["type"] = chunk_data['type']
                        if chunk_data.get('data'):
                            final_result["data"] = chunk_data['data']
                        if chunk_data.get('id'):
                            final_result["id"] = chunk_data['id']
                    except:
                        # If parsing fails, add raw content
                        final_result["content"] += chunk + "\n"
                else:
                    final_result["content"] += chunk + "\n"
            elif isinstance(chunk, dict):
                if chunk.get('content'):
                    final_result["content"] += chunk['content'] + "\n"
                if chunk.get('type'):
                    final_result["type"] = chunk['type']
                if chunk.get('data'):
                    final_result["data"] = chunk['data']
                if chunk.get('id'):
                    final_result["id"] = chunk['id']
                    
        # If we have specific content types, structure the response better
        if analysis_content:
            final_result["content"] = analysis_content.strip()
            
        # If no content was collected, use the last chunk
        if not final_result["content"] and result_data:
            last_chunk = result_data[-1]
            if isinstance(last_chunk, dict):
                final_result.update(last_chunk)
            else:
                final_result["content"] = str(last_chunk)
            
        return JSONResponse(content=final_result)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            content={'content': str(e), 'type': 'error'},
            status_code=500
        )


@router.post("/excel/export")
async def export_excel(excel_data: ExcelData, trans: Trans):
    def inner():
        _fields_list = []
        data = []
        if not excel_data.data:
            raise HTTPException(
                status_code=500,
                detail=trans("i18n_excel_export.data_is_empty")
            )

        for _data in excel_data.data:
            _row = []
            for field in excel_data.axis:
                _row.append(_data.get(field.value))
            data.append(_row)
        for field in excel_data.axis:
            _fields_list.append(field.name)
        df = pd.DataFrame(np.array(data), columns=_fields_list)

        buffer = io.BytesIO()

        with pd.ExcelWriter(buffer, engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_numbers': True}}) as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)

        buffer.seek(0)
        return io.BytesIO(buffer.getvalue())

    result = await asyncio.to_thread(inner)
    return JSONResponse(content={"message": "Excel export functionality is available via streaming response only"})
