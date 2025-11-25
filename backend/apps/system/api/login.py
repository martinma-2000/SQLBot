from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Form
from apps.system.schemas.system_schema import BaseUserDTO
from common.core.deps import SessionDep, Trans
from ..crud.user import authenticate, get_user_by_account
from common.core.security import create_access_token, md5pwd, default_md5_pwd
from datetime import timedelta
from common.core.config import settings
from common.core.schemas import Token
from ..models.user import UserModel
from ..models.system_model import UserWsModel
from sqlmodel import select
import re

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