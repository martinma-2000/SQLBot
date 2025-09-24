import asyncio
import hashlib
import os
import traceback
import uuid
from io import StringIO
from typing import List

import orjson
import pandas as pd
from fastapi import APIRouter, File, UploadFile, HTTPException, Form
from pydantic import BaseModel

from test.excel_extract import ExcelHeaderProcessor
from test.merge_diff_time import concatenate_dataframes
from test.merge_diff_column import merge_dataframes_horizontally


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


@router.get("/ws/{oid}", include_in_schema=False)
async def query_by_oid(session: SessionDep, user: CurrentUser, oid: int) -> List[CoreDatasource]:
    if not user.isAdmin:
        raise Exception("no permission to execute")
    return get_datasource_list(session=session, user=user, oid=oid)


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
#                 # build fields
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
    files: List[UploadFile] = File(...),
    separator: str = Form("_"),
    primary_key_col: int = Form(0)
):
    """
    拼接多个Excel文件
    
    参数:
    - files: 上传的多个Excel文件
    - separator: 连接符，默认为下划线
    - primary_key_col: 主键列索引，默认为0
    
    返回:
    - PreprocessResponse: 拼接后的文件信息
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
        
        # 拼接所有DataFrame
        result_df = concatenate_dataframes(dataframes, primary_key_col)
        
        # 保存拼接后的文件
        concatenated_filename = f"concatenated_{hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]}.xlsx"
        concatenated_path = os.path.join(path, concatenated_filename)
        result_df.to_excel(concatenated_path, index=False)
        
        # 返回拼接后的文件信息
        # sheets = [{"tableName": "Sheet1", "tableComment": "Concatenated Sheet"}]
        # return PreprocessResponse(filename=concatenated_filename, sheets=sheets)
        # 返回文件供下载
        return FileResponse(
            path=concatenated_path,
            filename=concatenated_filename,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        # 删除临时文件
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
        raise HTTPException(500, f"拼接文件时出错: {str(e)}")


@router.post("/mergeExcelsHorizontally")
async def merge_excels_horizontally(
    files: List[UploadFile] = File(...),
    separator: str = Form("_"),
    time_col: int = Form(0)
):
    """
    横向合并多个Excel文件，基于相同的时间列
    
    参数:
    - files: 上传的多个Excel文件
    - separator: 连接符，默认为下划线
    - time_col: 时间列索引，默认为0（第一列）
    
    返回:
    - 合并后的Excel文件
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
        
        # 返回文件供下载
        return FileResponse(
            path=merged_path,
            filename=merged_filename,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        # 删除临时文件
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
        raise HTTPException(500, f"横向合并文件时出错: {str(e)}")


def insert_pg(df, tableName, engine):
    # fix field type
    for i in range(len(df.dtypes)):
        if str(df.dtypes[i]) == 'uint64':
            df[str(df.columns[i])] = df[str(df.columns[i])].astype('string')

    # 生成字母序列作为列名
    def get_column_name(index):
        if index < 26:
            return chr(ord('A') + index)
        else:
            return chr(ord('A') + index // 26 - 1) + chr(ord('A') + index % 26)

    # 保存原始列名用于注释
    original_columns = df.columns.tolist()

    # 重命名列名为字母序列
    new_columns = [get_column_name(i) for i in range(len(df.columns))]
    df.columns = new_columns

    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        df.to_sql(
            tableName,
            engine,
            if_exists='replace',
            index=False
        )

        comment_queries = []
        for i, col_name in enumerate(new_columns):
            col_comment = original_columns[i].replace("'", "''")
            comment_queries.append(f"COMMENT ON COLUMN \"{tableName}\".\"{col_name}\" IS '{col_comment}'")
        for query in comment_queries:
            cursor.execute(query)
        # trans csv
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        # output.seek(0)

        # pg copy
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