import asyncio
import hashlib
import os
import traceback
import uuid
from io import StringIO
from typing import List
from enum import Enum

import orjson
import pandas as pd
from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Query
import requests
from pydantic import BaseModel

from excel_processing.excel_extract import ExcelHeaderProcessor
from excel_processing.merge_diff_time import concatenate_dataframes
from excel_processing.merge_diff_column import merge_dataframes_horizontally

from sqlalchemy.types import Date, Text

from apps.db.db import get_schema
from apps.db.engine import get_engine_conn
from common.core.config import settings
from common.core.deps import SessionDep, CurrentUser, Trans
from common.utils.utils import SQLBotLogUtil
from ..crud.datasource import get_datasource_list, check_status, create_ds, update_ds, delete_ds, getTables, getFields, \
    execSql, update_table_and_fields, getTablesByDs, chooseTables, preview, updateTable, updateField, get_ds, fieldEnum, \
    check_status_by_id
from ..crud.field import get_fields_by_table_id
from ..crud.table import get_tables_by_ds_id
from ..models.datasource import CoreDatasource, CreateDatasource, TableObj, CoreTable, CoreField

router = APIRouter(tags=["datasource"], prefix="/datasource")
path = settings.EXCEL_PATH


class PreprocessResponse(BaseModel):
    """预处理响应模型"""
    filename: str
    sheets: List[dict]


class ConcatenateRequest(BaseModel):
    """拼接请求模型"""
    file_paths: List[str]
    sheet_names: List[str] = None


class FetchApiRequest(BaseModel):
    """通过API拉取Excel的请求模型"""
    endpoint: str
    method: str = "GET"
    date_m: str | None = None
    p_date_m: str | None = None
    period_type: str | None = None  # 周期类型：day|month|quarter|year，默认month
    period: str | None = None       # 周期值，如 202411（month）、20241112（day）、2024Q4（quarter）
    headerKey: str | None = None
    headerValue: str | None = None
    cookieKey: str | None = None
    cookieValue: str | None = None
    paramKey: str | None = None
    paramValue: str | None = None
    timeout: int = 30
    separator: str = "_"


class TestApiResponse(BaseModel):
    """API连通性/下载测试响应模型"""
    ok: bool = True
    message: str = "API可达且返回Excel"
    ext: str | None = None
    sheet_names: List[str] | None = None
    filename: str | None = None


class DataSourceQuestionResponse(BaseModel):
    """数据源和问题响应模型"""
    datasource_id: int
    questions: List[str]


class CommonQuestionType(str, Enum):
    """常见问题类型枚举"""
    ASSETS = "assets"      # 资产
    INDICATORS = "indicators"  # 指标
    REPORTS = "reports"    # 报表


@router.get("/ws/{oid}", include_in_schema=False)
async def query_by_oid(session: SessionDep, user: CurrentUser, oid: int) -> List[CoreDatasource]:
    if not user.isAdmin:
        raise Exception("no permission to execute")
    return get_datasource_list(session=session, user=user, oid=oid)


@router.get("/common-questions", response_model=DataSourceQuestionResponse)
async def get_common_questions(
    session: SessionDep, 
    question_type: CommonQuestionType = Query(..., description="问题类型: assets(资产), indicators(指标), reports(报表)")
):
    """
    根据不同类型获取常见问题
    
    Args:
        session: 数据库会话
        question_type: 问题类型 (assets: 资产, indicators: 指标, reports: 报表)
        
    Returns:
        DataSourceQuestionResponse: 包含数据源ID和问题列表
        
    Examples:
        1. 资产类型: 返回问题 D, E, F (无数据源ID)
        2. 指标类型: 返回问题 G, H, I (无数据源ID)
        3. 报表类型: 返回数据源ID和问题 A, B, C
    """
    if question_type == CommonQuestionType.ASSETS:
        # 资产类型：返回问题D, E, F，数据源ID为0
        return DataSourceQuestionResponse(
            datasource_id=0,
            questions=["D", "E", "F"]
        )
    elif question_type == CommonQuestionType.INDICATORS:
        # 指标类型：返回问题G, H, I，数据源ID为0
        return DataSourceQuestionResponse(
            datasource_id=0,
            questions=["G", "H", "I"]
        )
    elif question_type == CommonQuestionType.REPORTS:
        # 报表类型：查找名为"存款测试"的数据源
        datasource = session.query(CoreDatasource).filter(CoreDatasource.name == "存款测试").first()
        
        # 如果找不到"存款测试"数据源，获取第一个可用的数据源
        if not datasource:
            datasource = session.query(CoreDatasource).first()
        
        # 如果仍然没有数据源，抛出异常
        if not datasource:
            raise HTTPException(status_code=404, detail="No datasource found")
        
        # 返回数据源ID和问题A, B, C
        return DataSourceQuestionResponse(
            datasource_id=datasource.id,
            questions=["A", "B", "C"]
        )


@router.get("/list")
async def datasource_list(session: SessionDep, user: CurrentUser):
    return get_datasource_list(session=session, user=user)


@router.post("/get/{id}")
async def get_datasource(session: SessionDep, id: int):
    return get_ds(session, id)


@router.post("/check")
async def check(session: SessionDep, trans: Trans, ds: CoreDatasource):
    def inner():
        return check_status(session, trans, ds, True)

    return await asyncio.to_thread(inner)


@router.get("/check/{ds_id}")
async def check_by_id(session: SessionDep, trans: Trans, ds_id: int):
    def inner():
        return check_status_by_id(session, trans, ds_id, True)

    return await asyncio.to_thread(inner)


@router.post("/add", response_model=CoreDatasource)
async def add(session: SessionDep, trans: Trans, user: CurrentUser, ds: CreateDatasource):
    def inner():
        return create_ds(session, trans, user, ds)

    return await asyncio.to_thread(inner)


@router.post("/chooseTables/{id}")
async def choose_tables(session: SessionDep, trans: Trans, id: int, tables: List[CoreTable]):
    def inner():
        chooseTables(session, trans, id, tables)

    await asyncio.to_thread(inner)


@router.post("/update", response_model=CoreDatasource)
async def update(session: SessionDep, trans: Trans, user: CurrentUser, ds: CoreDatasource):
    def inner():
        return update_ds(session, trans, user, ds)

    return await asyncio.to_thread(inner)


@router.post("/delete/{id}", response_model=CoreDatasource)
async def delete(session: SessionDep, id: int):
    return delete_ds(session, id)


class CreateDatasourceFromMergeResult(BaseModel):
    """根据合并结果创建数据源的请求模型"""
    name: str
    description: str = ""
    type: str = "excel"
    filename: str
    sheets: List[dict]
    
    
@router.post("/create-from-merge-result", response_model=CoreDatasource)
async def create_datasource_from_merge_result(
    session: SessionDep, 
    trans: Trans, 
    user: CurrentUser, 
    merge_result: CreateDatasourceFromMergeResult
):
    """
    根据concatenateExcels或mergeExcelsHorizontally的输出结果创建数据源记录
    
    参数:
    - name: 数据源名称
    - description: 数据源描述
    - type: 数据源类型，默认为"excel"
    - filename: 合并后的文件名
    - sheets: 表信息列表，每个元素包含tableName和tableComment
    
    返回:
    - CoreDatasource: 创建的数据源对象
    """
    # 构建configuration
    configuration = {
        "filename": merge_result.filename,
        "sheets": merge_result.sheets,
        "mode": "service_name"
    }
    
    # 加密configuration
    from ..utils.utils import aes_encrypt
    import json
    encrypted_config = aes_encrypt(json.dumps(configuration)).decode('utf-8')
    
    # 构建tables
    tables = []
    for sheet in merge_result.sheets:
        table = CoreTable(
            table_name=sheet["tableName"],
            table_comment=sheet.get("tableComment", "")
        )
        tables.append(table)
    
    # 构建CreateDatasource对象
    create_ds_obj = CreateDatasource(
        name=merge_result.name,
        description=merge_result.description,
        type=merge_result.type,
        configuration=encrypted_config,
        tables=tables
    )
    
    def inner():
        return create_ds(session, trans, user, create_ds_obj)
    
    return await asyncio.to_thread(inner)


@router.post("/getTables/{id}")
async def get_tables(session: SessionDep, id: int):
    return getTables(session, id)


@router.post("/getTablesByConf")
async def get_tables_by_conf(session: SessionDep, trans: Trans, ds: CoreDatasource):
    try:
        return getTablesByDs(session, ds)
    except Exception as e:
        # check ds status
        def inner():
            return check_status(session, trans, ds, True)

        status = await asyncio.to_thread(inner)
        if status:
            SQLBotLogUtil.error(f"get table failed: {e}")
            raise HTTPException(status_code=500, detail=f'Get table Failed: {e.args}')


@router.post("/getSchemaByConf")
async def get_schema_by_conf(session: SessionDep, trans: Trans, ds: CoreDatasource):
    try:
        return get_schema(ds)
    except Exception as e:
        # check ds status
        def inner():
            return check_status(session, trans, ds, True)

        status = await asyncio.to_thread(inner)
        if status:
            SQLBotLogUtil.error(f"get table failed: {e}")
            raise HTTPException(status_code=500, detail=f'Get table Failed: {e.args}')


@router.post("/getFields/{id}/{table_name}")
async def get_fields(session: SessionDep, id: int, table_name: str):
    return getFields(session, id, table_name)


from pydantic import BaseModel


class TestObj(BaseModel):
    sql: str = None


# not used, just do test
@router.post("/execSql/{id}")
async def exec_sql(session: SessionDep, id: int, obj: TestObj):
    def inner():
        data = execSql(session, id, obj.sql)
        try:
            data_obj = data.get('data')
            # print(orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS).decode())
            print(orjson.dumps(data_obj).decode())
        except Exception:
            traceback.print_exc()

        return data

    return await asyncio.to_thread(inner)


@router.post("/tableList/{id}")
async def table_list(session: SessionDep, id: int):
    return get_tables_by_ds_id(session, id)


@router.post("/fieldList/{id}")
async def field_list(session: SessionDep, id: int):
    return get_fields_by_table_id(session, id)


@router.post("/editLocalComment")
async def edit_local(session: SessionDep, data: TableObj):
    update_table_and_fields(session, data)


@router.post("/editTable")
async def edit_table(session: SessionDep, table: CoreTable):
    updateTable(session, table)


@router.post("/editField")
async def edit_field(session: SessionDep, field: CoreField):
    updateField(session, field)


@router.post("/previewData/{id}")
async def preview_data(session: SessionDep, trans: Trans, current_user: CurrentUser, id: int, data: TableObj):
    def inner():
        try:
            return preview(session, current_user, id, data)
        except Exception as e:
            ds = session.query(CoreDatasource).filter(CoreDatasource.id == id).first()
            # check ds status
            status = check_status(session, trans, ds, True)
            if status:
                SQLBotLogUtil.error(f"Preview failed: {e}")
                raise HTTPException(status_code=500, detail=f'Preview Failed: {e.args}')

    return await asyncio.to_thread(inner)


# not used
@router.post("/fieldEnum/{id}")
async def field_enum(session: SessionDep, id: int):
    def inner():
        return fieldEnum(session, id)

    return await asyncio.to_thread(inner)


# @router.post("/uploadExcel")
# async def upload_excel(session: SessionDep, file: UploadFile = File(...)):
#     ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv"}
#     if not file.filename.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
#         raise HTTPException(400, "Only support .xlsx/.xls/.csv")
#
#     os.makedirs(path, exist_ok=True)
#     filename = f"{file.filename.split('.')[0]}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.{file.filename.split('.')[1]}"
#     save_path = os.path.join(path, filename)
#     with open(save_path, "wb") as f:
#         f.write(await file.read())
#
#     def inner():
#         sheets = []
#         with get_data_engine() as conn:
#             if filename.endswith(".csv"):
#                 df = pd.read_csv(save_path, engine='c')
#                 tableName = f"sheet1_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}"
#                 sheets.append({"tableName": tableName, "tableComment": ""})
#                 column_len = len(df.dtypes)
#                 fields = []
#                 for i in range(column_len):
#                     # build fields
#                     fields.append({"name": df.columns[i], "type": str(df.dtypes[i]), "relType": ""})
#                 # create table
#                 create_table(conn, tableName, fields)
#
#                 data = [
#                     {df.columns[i]: None if pd.isna(row[i]) else (int(row[i]) if "int" in str(df.dtypes[i]) else row[i])
#                      for i in range(len(row))}
#                     for row in df.values
#                 ]
#                 # insert data
#                 insert_data(conn, tableName, fields, data)
#             else:
#                 excel_engine = 'xlrd' if filename.endswith(".xls") else 'openpyxl'
#                 df_sheets = pd.read_excel(save_path, sheet_name=None, engine=excel_engine)
#                 # build columns and data to insert db
#                 for sheet_name, df in df_sheets.items():
#                     tableName = f"{sheet_name}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}"
#                     sheets.append({"tableName": tableName, "tableComment": ""})
#                     column_len = len(df.dtypes)
#                     fields = []
#                     for i in range(column_len):
#                         # build fields
#                         fields.append({"name": df.columns[i], "type": str(df.dtypes[i]), "relType": ""})
#                     # create table
#                     create_table(conn, tableName, fields)
#
#                     data = [
#                         {df.columns[i]: None if pd.isna(row[i]) else (
#                             int(row[i]) if "int" in str(df.dtypes[i]) else row[i])
#                          for i in range(len(row))}
#                         for row in df.values
#                     ]
#                     # insert data
#                     insert_data(conn, tableName, fields, data)
#
#         os.remove(save_path)
#         return {"filename": filename, "sheets": sheets}
#
#     return await asyncio.to_thread(inner)


@router.post("/uploadExcel")
async def upload_excel(session: SessionDep, file: UploadFile = File(...)):
    ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv"}
    if not file.filename.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
        raise HTTPException(400, "Only support .xlsx/.xls/.csv")

    os.makedirs(path, exist_ok=True)
    filename = f"{file.filename.split('.')[0]}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.{file.filename.split('.')[1]}"
    save_path = os.path.join(path, filename)
    with open(save_path, "wb") as f:
        f.write(await file.read())

    def inner():
        sheets = []
        engine = get_engine_conn()
        if filename.endswith(".csv"):
            df = pd.read_csv(save_path, engine='c')
            tableName = f"sheet1_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}"
            sheets.append({"tableName": tableName, "tableComment": ""})
            insert_pg(df, tableName, engine)
        else:
            sheet_names = pd.ExcelFile(save_path).sheet_names
            for sheet_name in sheet_names:
                tableName = f"{sheet_name}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}"
                sheets.append({"tableName": tableName, "tableComment": ""})
                # df_temp = pd.read_excel(save_path, nrows=5)
                # non_empty_cols = df_temp.columns[df_temp.notna().any()].tolist()
                df = pd.read_excel(save_path, sheet_name=sheet_name, engine='calamine')
                insert_pg(df, tableName, engine)

        # os.remove(save_path)
        return {"filename": filename, "sheets": sheets}

    return await asyncio.to_thread(inner)


@router.post("/fetchExcelFromApi")
async def fetch_excel_from_api(session: SessionDep, req: FetchApiRequest):
    """
    通过API拉取Excel并进行预处理，然后与本地Excel上传流程一致：入库并返回sheet信息。

    入参:
    - endpoint: API地址
    - method: HTTP方法，默认GET
    - date_m/p_date_m: 作为查询参数传递（可选）
    - header/cookie/param: 单个认证或参数键值（可选）
    - timeout: 超时秒数
    - separator: 多级表头连接符
    """

    # 准备下载目录
    os.makedirs(path, exist_ok=True)

    # 构造请求
    url = req.endpoint
    params = {}
    if req.paramKey and req.paramValue:
        params[req.paramKey] = req.paramValue
    if req.date_m:
        params['date_m'] = req.date_m
    if req.p_date_m:
        params['p_date_m'] = req.p_date_m
    headers = {req.headerKey: req.headerValue} if req.headerKey and req.headerValue else None
    cookies = {req.cookieKey: req.cookieValue} if req.cookieKey and req.cookieValue else None

    # 生成原始下载文件名
    file_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]
    raw_ext = 'xlsx'
    raw_filename = f"{(req.p_date_m or 'api')}_{file_hash}.{raw_ext}"
    raw_save_path = os.path.join(path, raw_filename)

    # 下载Excel
    try:
        resp = requests.request(
            req.method.upper(),
            url,
            params=params if req.method.upper() == 'GET' else None,
            data=params if req.method.upper() != 'GET' else None,
            headers=headers,
            cookies=cookies,
            timeout=req.timeout,
        )
        resp.raise_for_status()

        # 尝试根据响应头判断文件扩展名
        content_disposition = resp.headers.get('Content-Disposition', '')
        if 'filename' in content_disposition:
            try:
                fname = content_disposition.split('filename=')[-1].strip('"')
                if '.' in fname:
                    raw_ext = fname.split('.')[-1].lower()
            except Exception:
                pass
        else:
            ctype = resp.headers.get('Content-Type', '')
            if 'spreadsheetml' in ctype or 'excel' in ctype:
                raw_ext = 'xlsx'
            elif 'csv' in ctype:
                raw_ext = 'csv'
            elif 'ms-excel' in ctype:
                raw_ext = 'xls'

        # 如果扩展名不同，更新保存路径
        raw_filename = f"{(req.p_date_m or 'api')}_{file_hash}.{raw_ext}"
        raw_save_path = os.path.join(path, raw_filename)

        with open(raw_save_path, 'wb') as f:
            f.write(resp.content)
    except Exception as e:
        raise HTTPException(400, f"下载Excel失败: {str(e)}")

    # 预处理（多级表头转单级）
    if raw_ext not in ("xlsx", "xls"):
        # 目前仅支持Excel文件
        if os.path.exists(raw_save_path):
            try:
                os.remove(raw_save_path)
            except Exception:
                pass
        raise HTTPException(400, "仅支持Excel文件类型（.xlsx/.xls）")
    try:
        processor = ExcelHeaderProcessor(separator=req.separator)
        df = processor.convert_multi_to_single_header(raw_save_path)
        processed_filename = raw_save_path.replace(f".{raw_ext}", "_processed.xlsx")
        df.to_excel(processed_filename, index=False)
    except Exception as e:
        if os.path.exists(raw_save_path):
            os.remove(raw_save_path)
        raise HTTPException(500, f"预处理文件时出错: {str(e)}")

    # 入库并返回sheets信息（单表 + 周期列）
    def inner():
        sheets = []
        engine = get_engine_conn()
        try:
            # 计算周期
            period_type = (req.period_type or 'month').lower()
            period_value = req.period or req.date_m or req.p_date_m
            if period_value is None:
                # 默认使用当前月（YYYYMM）
                from datetime import datetime
                period_value = datetime.today().strftime('%Y%m')

            # 计算稳定表名（同一URL配置归为同一张表）
            stable_key_parts = [req.method.upper(), req.endpoint or '']
            if req.headerKey and req.headerValue:
                stable_key_parts.append(f"H:{req.headerKey}={req.headerValue}")
            if req.cookieKey and req.cookieValue:
                stable_key_parts.append(f"C:{req.cookieKey}={req.cookieValue}")
            # 仅在非日期参数时参与稳定key
            if req.paramKey and req.paramValue and req.paramKey not in {'date_m', 'p_date_m'}:
                stable_key_parts.append(f"P:{req.paramKey}={req.paramValue}")
            stable_key = '|'.join(stable_key_parts)
            stable_hash = hashlib.sha256(stable_key.encode('utf-8')).hexdigest()[:8]
            tableName = f"api_{stable_hash}_data"

            # 读取所有sheet，合并到同一张表，追加周期列与sheet_name
            sheet_names = pd.ExcelFile(processed_filename).sheet_names
            for sheet_name in sheet_names:
                df_sheet = pd.read_excel(processed_filename, sheet_name=sheet_name, engine='calamine')
                # 添加周期列与 sheet_name，并将这三列放到倒数第5、倒数第4、倒数第3列
                df_sheet['period_type'] = period_type
                df_sheet['period'] = period_value
                df_sheet['sheet_name'] = sheet_name
                cols = list(df_sheet.columns)
                base_cols = [c for c in cols if c not in {'period_type', 'period', 'sheet_name'}]
                if len(base_cols) >= 2:
                    head = base_cols[:-2]
                    last_two = base_cols[-2:]
                    new_order = head + ['period_type', 'period', 'sheet_name'] + last_two
                else:
                    new_order = base_cols + ['period_type', 'period', 'sheet_name']
                df_sheet = df_sheet[new_order]
                insert_pg(df_sheet, tableName, engine, mode='append', preserve_columns=True, dedupe=True, dedupe_keys=['period'])

            # 响应返回统一表
            sheets.append({"tableName": tableName, "tableComment": "Single table with period columns"})
        finally:
            # 保留处理后的文件，便于复查；原始下载文件删除
            if os.path.exists(raw_save_path):
                try:
                    os.remove(raw_save_path)
                except Exception:
                    pass
            # 根据配置删除处理后的文件，避免 EXCEL_PATH 积累
            try:
                if not settings.KEEP_PROCESSED_EXCEL and os.path.exists(processed_filename):
                    os.remove(processed_filename)
            except Exception:
                pass
        return {"filename": os.path.basename(processed_filename), "sheets": sheets}

    return await asyncio.to_thread(inner)


@router.post("/testApiExcel", response_model=TestApiResponse)
async def test_api_excel(session: SessionDep, req: FetchApiRequest):
    """
    仅测试API连通性与Excel文件有效性：下载后检测文件类型并尝试解析sheet，不做入库与预处理。

    入参同 FetchApiRequest。
    返回: TestApiResponse
    """
    # 准备下载目录
    os.makedirs(path, exist_ok=True)

    # 构造请求
    url = req.endpoint
    params = {}
    if req.paramKey and req.paramValue:
        params[req.paramKey] = req.paramValue
    if req.date_m:
        params['date_m'] = req.date_m
    if req.p_date_m:
        params['p_date_m'] = req.p_date_m
    headers = {req.headerKey: req.headerValue} if req.headerKey and req.headerValue else None
    cookies = {req.cookieKey: req.cookieValue} if req.cookieKey and req.cookieValue else None

    # 临时文件名
    file_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]
    raw_ext = 'xlsx'
    raw_filename = f"{(req.p_date_m or 'api')}_{file_hash}.{raw_ext}"
    raw_save_path = os.path.join(path, raw_filename)

    # 下载并检测
    try:
        resp = requests.request(
            req.method.upper(),
            url,
            params=params if req.method.upper() == 'GET' else None,
            data=params if req.method.upper() != 'GET' else None,
            headers=headers,
            cookies=cookies,
            timeout=req.timeout,
        )
        resp.raise_for_status()

        # 判断扩展名
        content_disposition = resp.headers.get('Content-Disposition', '')
        ctype = resp.headers.get('Content-Type', '')
        if 'filename' in content_disposition:
            try:
                fname = content_disposition.split('filename=')[-1].strip('"')
                if '.' in fname:
                    raw_ext = fname.split('.')[-1].lower()
            except Exception:
                pass
        else:
            if 'spreadsheetml' in ctype or 'excel' in ctype:
                raw_ext = 'xlsx'
            elif 'ms-excel' in ctype:
                raw_ext = 'xls'
            elif 'csv' in ctype:
                raw_ext = 'csv'

        raw_filename = f"{(req.p_date_m or 'api')}_{file_hash}.{raw_ext}"
        raw_save_path = os.path.join(path, raw_filename)

        with open(raw_save_path, 'wb') as f:
            f.write(resp.content)
    except Exception as e:
        raise HTTPException(400, f"下载失败: {str(e)}")

    # 仅支持Excel
    if raw_ext not in ("xlsx", "xls"):
        if os.path.exists(raw_save_path):
            try:
                os.remove(raw_save_path)
            except Exception:
                pass
        raise HTTPException(400, "仅支持Excel文件类型（.xlsx/.xls）")

    # 尝试解析sheet
    try:
        sheet_names = pd.ExcelFile(raw_save_path).sheet_names
        # 进一步尝试读取首个sheet的一行，确保解析正常
        try:
            if sheet_names:
                _ = pd.read_excel(raw_save_path, sheet_name=sheet_names[0], nrows=1, engine='calamine')
        except Exception:
            # 读取行失败不致命，仍返回sheet名称供参考
            pass
        return TestApiResponse(
            ok=True,
            message="API可达且返回Excel",
            ext=raw_ext,
            sheet_names=sheet_names,
            filename=os.path.basename(raw_save_path)
        )
    except Exception as e:
        raise HTTPException(400, f"解析Excel失败: {str(e)}")
    finally:
        # 删除临时文件
        if os.path.exists(raw_save_path):
            try:
                os.remove(raw_save_path)
            except Exception:
                pass


@router.post("/preprocessExcel", response_model=PreprocessResponse)
async def preprocess_excel(
    file: UploadFile = File(...),
    separator: str = Form("_")
):
    """
    预处理Excel文件，将多级表头转换为单级表头
    
    参数:
    - file: 上传的Excel文件
    - separator: 连接符，默认为下划线
    
    返回:
    - PreprocessResponse: 预处理后的文件信息
    """
    ALLOWED_EXTENSIONS = {"xlsx", "xls"}
    if not file.filename.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
        raise HTTPException(400, "Only support .xlsx/.xls")

    os.makedirs(path, exist_ok=True)
    filename = f"{file.filename.split('.')[0]}_preprocessed_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.{file.filename.split('.')[1]}"
    save_path = os.path.join(path, filename)
    with open(save_path, "wb") as f:
        f.write(await file.read())

    try:
        # 创建处理器实例
        processor = ExcelHeaderProcessor(separator=separator)
        
        # 处理Excel文件，转换多级表头为单级表头
        df = processor.convert_multi_to_single_header(save_path)
        
        # 保存处理后的文件
        processed_filename = save_path.replace('.' + file.filename.split('.')[-1], '_processed.xlsx')
        df.to_excel(processed_filename, index=False)
        
        # 返回处理后的文件信息
        sheets = [{"tableName": "Sheet1", "tableComment": "Processed Sheet"}]
        return PreprocessResponse(filename=os.path.basename(processed_filename), sheets=sheets)
        
    except Exception as e:
        # 删除临时文件
        if os.path.exists(save_path):
            os.remove(save_path)
        raise HTTPException(500, f"预处理文件时出错: {str(e)}")

from fastapi.responses import FileResponse


@router.post("/concatenateExcels")
async def concatenate_excels(
        session: SessionDep,
        files: List[UploadFile] = File(...),
        separator: str = Form("_"),
        primary_key_col: int = Form(0)
):
    """
    拼接多个Excel文件并创建数据源

    参数:
    - files: 上传的多个Excel文件
    - separator: 连接符，默认为下划线
    - primary_key_col: 主键列索引，默认为0

    返回:
    - dict: 拼接后的文件信息和数据表信息
    """
    ALLOWED_EXTENSIONS = {"xlsx", "xls"}

    # 检查文件类型
    for file in files:
        if not file.filename.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
            raise HTTPException(400, "Only support .xlsx/.xls")

    # 保存上传的文件
    file_paths = []
    for file in files:
        filename = f"{file.filename.split('.')[0]}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.{file.filename.split('.')[1]}"
        save_path = os.path.join(path, filename)
        with open(save_path, "wb") as f:
            f.write(await file.read())
        file_paths.append(save_path)

    try:
        # 创建处理器实例
        processor = ExcelHeaderProcessor(separator=separator)

        # 预处理所有文件
        dataframes = []
        for file_path in file_paths:
            df = processor.convert_multi_to_single_header(file_path)
            # 处理包含"编码"的列，将其转换为字符串类型
            df = processor.convert_encoding_columns_to_str(df)
            dataframes.append(df)

        # 拼接所有DataFrame
        result_df = concatenate_dataframes(dataframes, primary_key_col)

        # 保存拼接后的文件
        concatenated_filename = f"concatenated_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.xlsx"
        concatenated_path = os.path.join(path, concatenated_filename)
        result_df.to_excel(concatenated_path, index=False)

        # 直接处理合并后的文件，类似uploadExcel的逻辑
        def inner():
            sheets = []
            engine = get_engine_conn()

            # 读取合并后的Excel文件
            sheet_names = pd.ExcelFile(concatenated_path).sheet_names
            for sheet_name in sheet_names:
                tableName = f"{sheet_name}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}"
                sheets.append({"tableName": tableName, "tableComment": "Concatenated data"})
                df = pd.read_excel(concatenated_path, sheet_name=sheet_name, engine='calamine')
                insert_pg(df, tableName, engine)

            return {"filename": concatenated_filename, "sheets": sheets}

        result = await asyncio.to_thread(inner)

        # 清理临时文件
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)

        return result
    except Exception as e:
        # 删除临时文件
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
        if 'concatenated_path' in locals() and os.path.exists(concatenated_path):
            os.remove(concatenated_path)
        raise HTTPException(500, f"拼接文件时出错: {str(e)}")


@router.post("/mergeExcelsHorizontally")
async def merge_excels_horizontally(
    session: SessionDep,
    files: List[UploadFile] = File(...),
    separator: str = Form("_"),
    time_col: int = Form(0)
):
    """
    横向合并多个Excel文件，基于相同的时间列并创建数据源

    参数:
    - files: 上传的多个Excel文件
    - separator: 连接符，默认为下划线
    - time_col: 时间列索引，默认为0（第一列）

    返回:
    - dict: 合并后的文件信息和数据表信息
    """
    ALLOWED_EXTENSIONS = {"xlsx", "xls"}

    # 检查文件类型
    for file in files:
        if not file.filename.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
            raise HTTPException(400, "Only support .xlsx/.xls")

    # 保存上传的文件
    file_paths = []
    for file in files:
        filename = f"{file.filename.split('.')[0]}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.{file.filename.split('.')[1]}"
        save_path = os.path.join(path, filename)
        with open(save_path, "wb") as f:
            f.write(await file.read())
        file_paths.append(save_path)

    try:
        # 创建处理器实例
        processor = ExcelHeaderProcessor(separator=separator)

        # 预处理所有文件
        dataframes = []
        for file_path in file_paths:
            df = processor.convert_multi_to_single_header(file_path)
            dataframes.append(df)

        # 横向合并所有DataFrame
        result_df = merge_dataframes_horizontally(dataframes, time_col)

        # 保存合并后的文件
        merged_filename = f"merged_horizontally_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.xlsx"
        merged_path = os.path.join(path, merged_filename)
        result_df.to_excel(merged_path, index=False)

        # 直接处理合并后的文件，类似uploadExcel的逻辑
        def inner():
            sheets = []
            engine = get_engine_conn()

            # 读取合并后的Excel文件
            sheet_names = pd.ExcelFile(merged_path).sheet_names
            for sheet_name in sheet_names:
                tableName = f"{sheet_name}_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}"
                sheets.append({"tableName": tableName, "tableComment": "Merged horizontally data"})
                df = pd.read_excel(merged_path, sheet_name=sheet_name, engine='calamine')
                insert_pg(df, tableName, engine)

            return {"filename": merged_filename, "sheets": sheets}

        result = await asyncio.to_thread(inner)

        # 清理临时文件
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)

        return result
    except Exception as e:
        # 删除临时文件
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
        if 'merged_path' in locals() and os.path.exists(merged_path):
            os.remove(merged_path)
        raise HTTPException(500, f"横向合并文件时出错: {str(e)}")


def insert_pg(df, tableName, engine, mode: str = 'replace', preserve_columns: bool = False, dedupe: bool = False, dedupe_keys: list[str] | None = None):
    """将 DataFrame 插入 PG。

    - mode='replace': 保持原有行为（替换表结构并写入数据），列名重命名为字母序列。
    - mode='append': 若表不存在则按当前列创建表（不写入数据），随后使用 COPY 追加行；可选保留原列名。
    """

    # 修正可能的 uint64 类型为字符串，避免 PG 不兼容
    for i in range(len(df.dtypes)):
        if str(df.dtypes[i]) == 'uint64':
            df[str(df.columns[i])] = df[str(df.columns[i])].astype('string')

    # 将包含"编码"的列转换为字符串类型，避免被识别为数值类型
    for col in df.columns:
        if "编码" in str(col):
            df[col] = df[col].astype('string')

    # 列名处理
    original_columns = df.columns.tolist()

    def get_column_name(index):
        if index < 26:
            return chr(ord('A') + index)
        else:
            return chr(ord('A') + index // 26 - 1) + chr(ord('A') + index % 26)

    def normalize_name(name: str) -> str:
        # 保留 period 列的原名；其他列做简单规范化
        if name in {'period_type', 'period', 'sheet_name'}:
            return name
        name = str(name)
        # 去除两端空白，替换空白和特殊字符为下划线
        import re
        name = name.strip()
        name = re.sub(r"[^0-9A-Za-z_]+", "_", name)
        name = re.sub(r"_+", "_", name)
        # 如果规范化结果为纯下划线或空，回退为通用列名
        if re.fullmatch(r"_+", name) or name == "":
            name = "col"
        # 防止空列名
        return name or 'col'

    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        if mode == 'append':
            # 追加模式：保留列名（或规范化），不存在则建表
            if preserve_columns:
                # 规范化并确保列名唯一
                raw_cols = [normalize_name(c) for c in original_columns]
                seen = {}
                new_columns = []
                for col in raw_cols:
                    base = col
                    if base not in seen:
                        seen[base] = 1
                        new_columns.append(base)
                    else:
                        # 追加递增后缀，避免与已存在列冲突
                        idx = seen[base]
                        candidate = f"{base}_{idx}"
                        while candidate in seen:
                            idx += 1
                            candidate = f"{base}_{idx}"
                        seen[base] = idx + 1
                        seen[candidate] = 1
                        new_columns.append(candidate)
                df.columns = new_columns
            else:
                new_columns = [get_column_name(i) for i in range(len(df.columns))]
                df.columns = new_columns

            # 检查表是否存在
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=%s)",
                (tableName,)
            )
            exists = cursor.fetchone()[0]

            if not exists:
                # 创建空表结构（不写入数据），以便后续 COPY 追加
                dtype_dict = {}
                # 针对常规约定列设定类型
                for col in new_columns:
                    if col in {'period_type', 'period', 'sheet_name'}:
                        dtype_dict[col] = Text
                # 若原逻辑依赖最后两列为日期相关，尽量维持（仅在未保留列名时）
                if not preserve_columns and len(new_columns) >= 2:
                    dtype_dict[new_columns[-2]] = Text
                    dtype_dict[new_columns[-1]] = Date

                # 仅创建结构
                df.head(0).to_sql(
                    tableName,
                    engine,
                    if_exists='replace',
                    index=False,
                    dtype=dtype_dict
                )

                # 为列添加注释（保留原始列名痕迹）
                comment_queries = []
                for i, col_name in enumerate(new_columns):
                    col_comment = original_columns[i]
                    if isinstance(col_comment, str):
                        col_comment = col_comment.replace("'", "''")
                    else:
                        col_comment = str(col_comment)
                    comment_queries.append(
                        f"COMMENT ON COLUMN \"{tableName}\".\"{col_name}\" IS '{col_comment}'"
                    )
                for query in comment_queries:
                    cursor.execute(query)

            else:
                # 表已存在时，确保列顺序匹配表结构
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position",
                    (tableName,)
                )
                existing_cols = [r[0] for r in cursor.fetchall()]
                # 尝试对齐 DataFrame 列顺序；只选择存在的列，避免多余列导致 COPY 失败
                # 若缺少列，抛错给调用者（保持简单一致性）
                missing = [c for c in existing_cols if c not in df.columns]
                if missing:
                    raise HTTPException(400, f"待追加数据缺少必需列: {missing}")
                df = df[existing_cols]
                new_columns = existing_cols

            # 先在批次内去重（若指定了去重键，则按子集去重）
            # 注意：当仅以 period 作为去重键时，批次内所有行 period 值相同，
            # 若直接按 period 去重会导致只保留一行。此场景应改用表级去重（NOT EXISTS），
            # 以实现“首批同月全量写入，后续同月跳过”。
            if dedupe:
                do_batch_dedupe = True
                subset_cols = None
                if dedupe_keys:
                    subset_cols = [c for c in dedupe_keys if c in df.columns]
                    if not subset_cols:
                        subset_cols = None
                    # 当仅按 period 作为键时，跳过批次内去重，交由表级去重控制
                    if subset_cols and set(subset_cols) == {"period"}:
                        do_batch_dedupe = False
                if do_batch_dedupe:
                    df = df.drop_duplicates(subset=subset_cols)

            if not exists:
                # 目标表不存在：直接 COPY 到目标表
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cursor.copy_expert(
                    sql=f"""COPY "{tableName}" FROM STDIN WITH CSV DELIMITER E'\t'""",
                    file=output
                )
                conn.commit()
            else:
                # 目标表存在：使用阶段表 + 反连接插入避免重复
                import uuid as _uuid
                stage_table = f"__stage_{_uuid.uuid4().hex[:10]}"

                # 创建阶段表结构
                df.head(0).to_sql(
                    stage_table,
                    engine,
                    if_exists='replace',
                    index=False
                )

                # COPY 到阶段表
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cursor.copy_expert(
                    sql=f"""COPY "{stage_table}" FROM STDIN WITH CSV DELIMITER E'\t'""",
                    file=output
                )

                # 反连接插入新行（若 dedupe=False，则相当于全量插入）
                cols_quoted = ', '.join([f'"{c}"' for c in new_columns])
                if dedupe:
                    # 使用指定的去重键构造反连接条件；未指定则使用全部列
                    if dedupe_keys:
                        keys = [c for c in dedupe_keys if c in new_columns]
                        if not keys:
                            keys = new_columns
                    else:
                        keys = new_columns
                    join_cond = ' AND '.join([f't."{c}" = s."{c}"' for c in keys])
                    insert_sql = (
                        f"INSERT INTO \"{tableName}\" ({cols_quoted}) "
                        f"SELECT {cols_quoted} FROM \"{stage_table}\" s "
                        f"WHERE NOT EXISTS (SELECT 1 FROM \"{tableName}\" t WHERE {join_cond})"
                    )
                else:
                    insert_sql = (
                        f"INSERT INTO \"{tableName}\" ({cols_quoted}) "
                        f"SELECT {cols_quoted} FROM \"{stage_table}\""
                    )
                cursor.execute(insert_sql)

                # 删除阶段表
                cursor.execute(f'DROP TABLE "{stage_table}"')
                conn.commit()

        else:
            # 替换模式：保持旧行为（重命名为字母列 + to_sql 替换 + 注释 + COPY）
            new_columns = [get_column_name(i) for i in range(len(df.columns))]
            df.columns = new_columns

            dtype_dict = {}
            if len(new_columns) >= 2:
                dtype_dict[new_columns[-2]] = Text  # 倒数第二列：表格日期_source
                dtype_dict[new_columns[-1]] = Date  # 最后一列：表格日期

            # 仅替换表结构，实际数据通过 COPY 插入一次，避免重复
            df.head(0).to_sql(
                tableName,
                engine,
                if_exists='replace',
                index=False,
                dtype=dtype_dict
            )

            # 列注释保留原始列名
            comment_queries = []
            for i, col_name in enumerate(new_columns):
                col_comment = original_columns[i].replace("'", "''")
                comment_queries.append(
                    f"COMMENT ON COLUMN \"{tableName}\".\"{col_name}\" IS '{col_comment}'"
                )
            for query in comment_queries:
                cursor.execute(query)

            # COPY 再插入一遍（维持与旧实现一致的效果）
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cursor.copy_expert(
                sql=f"""COPY "{tableName}" FROM STDIN WITH CSV DELIMITER E'\t'""",
                file=output
            )
            conn.commit()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(400, str(e))
    finally:
        cursor.close()
        conn.close()
