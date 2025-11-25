from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from apps.system.schemas.system_schema import BaseUserDTO
from common.core.deps import SessionDep, Trans
from ..crud.user import authenticate
from common.core.security import create_access_token
from datetime import timedelta
from common.core.config import settings
from common.core.schemas import Token
router = APIRouter(tags=["login"], prefix="/login")

@router.post("/access-token")
async def local_login(
    session: SessionDep,
    trans: Trans,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
) -> Token:
    # 直接使用明文用户名和密码，不再进行解密操作
    user: BaseUserDTO = authenticate(session=session, account=form_data.username, password=form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail=trans('i18n_login.account_pwd_error'))
    if not user.oid or user.oid == 0:
        raise HTTPException(status_code=400, detail=trans('i18n_login.no_associated_ws', msg = trans('i18n_concat_admin')))
    if user.status != 1:
        raise HTTPException(status_code=400, detail=trans('i18n_login.user_disable', msg = trans('i18n_concat_admin')))
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    user_dict = user.to_dict()
    return Token(access_token=create_access_token(
        user_dict, expires_delta=access_token_expires
    ))