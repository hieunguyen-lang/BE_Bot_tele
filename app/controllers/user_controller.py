from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from ..database import get_db
from ..models import User, UserRole
from ..schemas import User as UserSchema, UserCreate, UserUpdate
from ..auth import get_current_active_user, get_current_admin_user
from ..services import user_service

router = APIRouter()

@router.post("/create_user")
async def create_user(
    user: UserCreate,
    db: AsyncSession = Depends(get_db),
    #current_user: User = Depends(get_current_admin_user)  # Chỉ admin mới có thể tạo user
):
    db_user = await user_service.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    db_user = await user_service.get_user_by_username(db, username=user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    return await user_service.create_user(db=db, user=user)

@router.get("/")
async def read_users(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)  # Chỉ admin mới có thể xem danh sách user
):
    users = await user_service.get_users(db, skip=skip, limit=limit)
    return users

@router.get("/me")
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return {"status": "success","role":current_user.role}

@router.get("/{user_id}", response_model=UserSchema)
async def read_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)  # Chỉ admin mới có thể xem thông tin user khác
):
    db_user = await user_service.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@router.patch("/{user_id}", response_model=UserSchema)
async def update_user(
    user_id: int,
    user: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)  # Chỉ admin mới có thể cập nhật user
):
    return await user_service.update_user(db=db, user_id=user_id, user=user)

@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)  # Chỉ admin mới có thể xóa user
):
    return await user_service.delete_user(db=db, user_id=user_id) 