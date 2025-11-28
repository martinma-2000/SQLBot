import asyncio
import json
from typing import List, Dict, Any

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from sqlalchemy import text

from apps.db.engine import get_data_engine

# 创建路由实例
router = APIRouter(tags=["custom-query"], prefix="/custom-query")


class QueryRequest(BaseModel):
    CURNT_PGQT: int
    CURNT_PG_KPRCD_QTY: int
    QRY_CNDT_JSSTR: str


class QueryResponse(BaseModel):
    CURNT_PGQT: int
    BDP_QRY_OBJ_ASBG: List[Dict[str, Any]]
    NXTPG_FLG: str
    PRVPG_FLG: str
    KPRCD_OVRAL_QTY: int


@router.post("/query", response_model=QueryResponse)
async def query_data(request: QueryRequest):
    """
    特殊查询接口，处理指定格式的查询请求
    
    入参格式:
    {
      "CURNT_PGQT": 1,
      "CURNT_PG_KPRCD_QTY": 20,
      "QRY_CNDT_JSSTR": "{\"ID\": \"L.csttranshis\", \"CONDT\": {\"AND\": [{\"txndt\": {\"GE\": \"20150101\", \"LT\": \"20160106\"}}, {\"txnccy\": {\"EQ\": \"01\"}}], \"OR\": [{\"orgid\": {\"EQ\": \"27080608\"}}]}, \"RESP\": [\"acctid\", \"orgid\", \"certno\", \"remark\"], \"ORDER\": {\"acctid\": \"ASC\", \"orgid\": \"DESC\"}}"
    }
    
    出参格式:
    {
      "CURNT_PGQT": 1,
      "BDP_QRY_OBJ_ASBG": [
        {
          "acctid": "100001",
          "orgid": "27080608",
          "certno": "110101199001011234",
          "remark": "正常交易"
        },
        ...
      ],
      "NXTPG_FLG": "1",
      "PRVPG_FLG": "0",
      "KPRCD_OVRAL_QTY": 156
    }
    """
    
    def inner():
        # 解析查询条件
        query_condition = json.loads(request.QRY_CNDT_JSSTR)
        
        # 提取查询参数
        table_id = query_condition.get("ID", "")
        conditions = query_condition.get("CONDT", {})
        response_fields = query_condition.get("RESP", [])
        order_by = query_condition.get("ORDER", {})
        
        # 构建SQL查询语句
        sql_parts = []
        params = {}
        
        # SELECT子句
        fields_str = ", ".join(response_fields) if response_fields else "*"
        sql_parts.append(f"SELECT {fields_str}")
        
        # FROM子句 (这里假设table_id格式为"schema.table")
        table_parts = table_id.split(".")[-1] if "." in table_id else table_id
        sql_parts.append(f"FROM {table_parts}")
        
        # WHERE子句
        where_conditions = []
        
        # 处理AND条件
        if "AND" in conditions:
            for i, and_condition in enumerate(conditions["AND"]):
                for field, condition in and_condition.items():
                    if "GE" in condition:  # 大于等于
                        param_name = f"{field}_ge_{i}"
                        where_conditions.append(f"{field} >= :{param_name}")
                        params[param_name] = condition["GE"]
                    if "LT" in condition:  # 小于
                        param_name = f"{field}_lt_{i}"
                        where_conditions.append(f"{field} < :{param_name}")
                        params[param_name] = condition["LT"]
                    if "EQ" in condition:  # 等于
                        param_name = f"{field}_eq_{i}"
                        where_conditions.append(f"{field} = :{param_name}")
                        params[param_name] = condition["EQ"]
        
        # 处理OR条件
        if "OR" in conditions:
            or_conditions = []
            for i, or_condition in enumerate(conditions["OR"]):
                for field, condition in or_condition.items():
                    if "EQ" in condition:  # 等于
                        param_name = f"{field}_or_eq_{i}"
                        or_conditions.append(f"{field} = :{param_name}")
                        params[param_name] = condition["EQ"]
            if or_conditions:
                where_conditions.append(f"({' OR '.join(or_conditions)})")
        
        # 添加WHERE子句
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        # ORDER BY子句
        if order_by:
            order_parts = []
            for field, direction in order_by.items():
                order_parts.append(f"{field} {direction}")
            sql_parts.append(f"ORDER BY {', '.join(order_parts)}")
        
        # LIMIT和OFFSET子句用于分页
        limit = request.CURNT_PG_KPRCD_QTY
        offset = (request.CURNT_PGQT - 1) * limit
        sql_parts.append(f"LIMIT {limit} OFFSET {offset}")
        
        # 构建完整SQL
        sql = " ".join(sql_parts)
        
        # 为了测试目的，我们模拟一些数据而不是真正查询数据库
        # 在实际生产环境中，您可以取消下面的注释并使用真实的数据库查询
        
        # 模拟数据 - 替代真实数据库查询
        simulated_data = [
            {
                "acctid": "100001",
                "orgid": "27080608",
                "certno": "110101199001011234",
                "remark": "正常交易"
            },
            {
                "acctid": "100002",
                "orgid": "27080608",
                "certno": "110101199001011235",
                "remark": "大额交易"
            },
            {
                "acctid": "100003",
                "orgid": "27080609",
                "certno": "110101199001011236",
                "remark": "普通交易"
            }
        ]
        
        # 只返回请求页面的数据
        start_index = offset
        end_index = start_index + limit
        page_data = simulated_data[start_index:end_index]
        
        # 模拟总记录数
        total_count = len(simulated_data)
        
        # 判断是否有下一页和上一页
        has_next = total_count > (request.CURNT_PGQT * limit)
        has_prev = request.CURNT_PGQT > 1
        
        # 构建响应
        response = QueryResponse(
            CURNT_PGQT=request.CURNT_PGQT,
            BDP_QRY_OBJ_ASBG=page_data,
            NXTPG_FLG="1" if has_next else "0",
            PRVPG_FLG="1" if has_prev else "0",
            KPRCD_OVRAL_QTY=total_count
        )
        
        return response
    
    return await asyncio.to_thread(inner)


# 创建独立的FastAPI应用
app = FastAPI(title="Custom Query API", description="处理特殊格式查询请求的API")
app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)