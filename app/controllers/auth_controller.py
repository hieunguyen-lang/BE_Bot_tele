from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import timedelta
from app.schemas.auth import Token
from app.database import get_db
from app.services import user_service
from app import auth
from fastapi.responses import JSONResponse

router = APIRouter()

@router.post("/token")
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db)
):
    user = await user_service.authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"email": user.email, "role": user.role,"username": user.username}, 
        expires_delta=access_token_expires
    )
    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        samesite="lax",
        path="/",
        max_age=3600,  # optional: thời gian sống cookie
        secure=False   # hoặc True nếu dùng HTTPS
    )

    return response

@router.post("/logout")
async def logout(response: JSONResponse):
    response.delete_cookie("access_token", path="/")
    return {"message": "Logged out"}