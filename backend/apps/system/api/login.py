from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Form
from apps.system.schemas.system_schema import BaseUserDTO
from common.core.deps import SessionDep, Trans
from ..crud.user import authenticate, get_user_by_account
from common.core.security import create_access_token, md5pwd, default_md5_pwd
from datetime import timedelta, datetime
from common.core.config import settings
from common.core.schemas import Token
from ..models.user import UserModel
from ..models.system_model import UserWsModel
from sqlmodel import select
import re
import json
# Import DsRules model
from sqlbot_xpack.permissions.models.ds_rules import DsRules
# Import DsPermission model
from sqlbot_xpack.permissions.models.ds_permission import DsPermission
from apps.datasource.models.datasource import CoreDatasource, CoreTable, CoreField

router = APIRouter(tags=["login"], prefix="/login")

@router.post("/access-token")
async def local_login(
    session: SessionDep,
    trans: Trans,
    username: Annotated[str, Form()],
    org_id: Annotated[int, Form()] = 1
) -> Token:
    # 固定密码为 SQLBot@123456
    fixed_password = "SQLBot@123456"
    
    # 直接使用明文用户名和固定密码进行认证
    user: BaseUserDTO = authenticate(session=session, account=username, password=fixed_password)
    
    # 如果认证失败，检查是否是因为用户不存在
    if not user:
        # 尝试查找用户是否存在
        db_user = get_user_by_account(session=session, account=username)
        if not db_user:
            # 用户不存在，创建新用户
            # 生成邮箱，使用sxnx.com域名
            email_domain = "sxnx.com"
            # 简单处理邮箱，将用户名中的非字母数字字符替换为下划线
            clean_account = re.sub(r'[^a-zA-Z0-9]', '_', username)
            email = f"{clean_account}@{email_domain}"
            
            # 固定默认工作空间ID为1
            default_workspace_id = 1
            
            # 创建新用户，使用固定的密码
            new_user = UserModel(
                account=username,
                name=username,
                password=md5pwd(fixed_password),  # 使用MD5加密存储固定密码
                email=email,
                oid=default_workspace_id,  # 固定使用默认工作空间ID
                status=1  # 默认启用状态
            )
            
            session.add(new_user)
            session.commit()
            session.refresh(new_user)
            
            # 将新用户添加到默认工作空间（通过UserWsModel）
            user_ws = UserWsModel(
                uid=new_user.id,
                oid=default_workspace_id,  # 固定使用默认工作空间ID
                weight=0     # 默认权重
            )
            session.add(user_ws)
            session.commit()
            
            # Automatically add user to permission group based on org_id
            # Check if a rule group with the org_id name exists
            org_id_str = str(org_id)
            ds_rules = session.exec(select(DsRules).where(DsRules.name == org_id_str)).first()
            
            if ds_rules:
                # If rule group exists, add user to it
                try:
                    user_list = json.loads(ds_rules.user_list) if ds_rules.user_list else []
                except (json.JSONDecodeError, TypeError):
                    user_list = []
                
                # 确保使用用户ID的字符串形式而不是整数形式添加到规则组
                user_id_str = str(new_user.id)
                if user_id_str not in user_list:
                    user_list.append(user_id_str)
                    ds_rules.user_list = json.dumps(user_list)
                    session.add(ds_rules)
                    session.commit()
            else:
                # If rule group doesn't exist, create a new one with org_id as name
                new_rule_group = DsRules(
                    enable=True,
                    name=org_id_str,
                    description=f"Permission group for organization {org_id_str}",
                    user_list=json.dumps([str(new_user.id)]),  # 使用用户ID的字符串形式
                    permission_list="[]",
                    oid=default_workspace_id,
                    create_time=datetime.now()
                )
                session.add(new_rule_group)
                session.commit()
                session.refresh(new_rule_group)
                
                # Create permission rule based on org_id
                # Check if org_id ends with 99
                rule_name = ""
                search_value = ""
                if org_id_str.endswith("99") and len(org_id_str) >= 8:
                    # Extract first 6 digits
                    rule_name = org_id_str[:6]
                    search_value = org_id_str[:6]
                else:
                    # Use full 8-digit org_id
                    rule_name = org_id_str
                    search_value = org_id_str
                
                # Find all datasources
                datasources = session.exec(select(CoreDatasource)).all()
                permission_ids = []
                
                for ds in datasources:
                    # Find tables with fields containing "编码" (code)
                    tables = session.exec(select(CoreTable).where(CoreTable.ds_id == ds.id)).all()
                    for table in tables:
                        # Check if table has fields with "编码" in field_comment or field_name
                        fields = session.exec(select(CoreField).where(
                            CoreField.table_id == table.id,
                            (CoreField.field_comment.like("%编码%")) | (CoreField.field_name.like("%编码%"))
                        )).all()
                        
                        if fields:
                            # Create expression tree for the permission rule
                            expression_tree = {
                                "logic": "or",
                                "items": [{
                                    "enum_value": [],
                                    "field_id": fields[0].id,
                                    "filter_type": "logic",
                                    "term": "like",
                                    "value": search_value,
                                    "type": "item",
                                    "sub_tree": None
                                }]
                            }
                            
                            # Create new permission rule
                            permission_rule = DsPermission(
                                enable=True,
                                name=f"Permission rule for {rule_name}",
                                type="row",
                                ds_id=ds.id,
                                table_id=table.id,
                                expression_tree=json.dumps(expression_tree),
                                permissions="[]",
                                oid=default_workspace_id,
                                create_time=datetime.now(),
                                auth_target_type="ds_rules",
                                auth_target_id=new_rule_group.id
                            )
                            session.add(permission_rule)
                            session.commit()
                            session.refresh(permission_rule)
                            permission_ids.append(permission_rule.id)
                
                # Update the rule group with permission list
                if permission_ids:
                    new_rule_group.permission_list = json.dumps(permission_ids)
                    session.add(new_rule_group)
                    session.commit()
            
            # 直接使用新创建的用户信息创建BaseUserDTO对象
            # 注意：UserModel没有creator、updater等BaseCreatorDTO字段，所以我们只传递实际存在的字段
            user = BaseUserDTO(
                id=new_user.id,
                account=new_user.account,
                oid=new_user.oid,
                name=new_user.name,
                language=new_user.language,
                password=new_user.password,
                status=new_user.status,
                create_time=new_user.create_time
            )
        else:
            # 用户存在但密码错误，使用查询到的用户信息创建BaseUserDTO对象
            user = BaseUserDTO(
                id=db_user.id,
                account=db_user.account,
                oid=db_user.oid,
                name=db_user.name,
                language=db_user.language,
                password=db_user.password,
                status=db_user.status,
                create_time=db_user.create_time
            )
    
    if not user:
        raise HTTPException(status_code=400, detail=trans('i18n_login.account_pwd_error'))
    # 确保用户关联到默认工作空间
    if not user.oid or user.oid == 0:
        # 如果用户oid为0，则使用默认工作空间ID
        user.oid = 1
    if user.status != 1:
        raise HTTPException(status_code=400, detail=trans('i18n_login.user_disable', msg = trans('i18n_concat_admin')))
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    user_dict = user.to_dict()
    return Token(access_token=create_access_token(
        user_dict, expires_delta=access_token_expires
    ))