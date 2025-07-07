from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing import List
from ..database import get_db
from ..models import User, UserRole
from ..schemas import User as UserSchema, UserCreate, UserUpdate
from ..auth import get_current_active_user, get_current_admin_user
from ..services import user_service
from app.auth_permission import require_permission
from app.services.permission_service import get_user_permissions
from sqlalchemy.future import select
from app.models.permission import Permission
from app.models.role_permission import RolePermission
from app.models.user_permission import UserPermission

router = APIRouter()

@router.post("/create_user")
async def create_user(
    user: UserCreate,
    db: AsyncSession = Depends(get_db),
    perm: bool = Depends(require_permission("user:create"))
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
    perm: bool = Depends(require_permission("user:read"))
):
    users = await user_service.get_users(db, skip=skip, limit=limit)
    return users

@router.get("/me")
async def read_users_me(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    permissions = await get_user_permissions(db, current_user.id)
    return {
        "id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "role": current_user.role,
        "permissions": permissions,
        "is_active": current_user.is_active
    }

@router.get("/{user_id}", response_model=UserSchema)
async def read_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    perm: bool = Depends(require_permission("user:read"))
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
    perm: bool = Depends(require_permission("user:update"))
):
    return await user_service.update_user(db=db, user_id=user_id, user=user)

@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    perm: bool = Depends(require_permission("user:delete"))
):
    return await user_service.delete_user(db=db, user_id=user_id)

async def get_user_permissions(db: AsyncSession, user_id: int):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return []
    # Permission từ role
    role_permissions = []
    if user.role_id:
        result = await db.execute(
            select(Permission)
            .join(RolePermission, Permission.id == RolePermission.permission_id)
            .where(RolePermission.role_id == user.role_id)
        )
        role_permissions = result.scalars().all()
    # Permission gán trực tiếp
    result = await db.execute(
        select(Permission)
        .join(UserPermission, Permission.id == UserPermission.permission_id)
        .where(UserPermission.user_id == user_id)
    )
    user_permissions = result.scalars().all()
    permission_set = {p.name for p in (role_permissions + user_permissions)}
    return list(permission_set) 