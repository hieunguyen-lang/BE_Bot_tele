from fastapi import APIRouter, Depends, HTTPException, status,Body
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
from app.schemas.permission import PermissionOut

router = APIRouter()
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

@router.get("/permissions", response_model=List[PermissionOut])
async def get_all_permissions(
    db: AsyncSession = Depends(get_db),
    perm: bool = Depends(require_permission("user:read"))
):
    result = await db.execute(select(Permission))
    permissions = result.scalars().all()
    return permissions

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
    permissions = await get_user_permissions(db, user_id)
    user_dict = UserSchema.from_orm(db_user).dict()
    user_dict['permissions'] = permissions
    return user_dict

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

@router.post("/{user_id}/add_permission")
async def add_permission_to_user(
    user_id: int,
    permission_name: str = Body(..., embed=True),
    db: AsyncSession = Depends(get_db),
    perm: bool = Depends(require_permission("user:update"))
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    result = await db.execute(select(Permission).where(Permission.name == permission_name))
    permission = result.scalar_one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="Permission not found")
    result = await db.execute(
        select(UserPermission).where(
            UserPermission.user_id == user_id,
            UserPermission.permission_id == permission.id
        )
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="User already has this permission")
    user_permission = UserPermission(user_id=user_id, permission_id=permission.id)
    db.add(user_permission)
    await db.commit()
    return {"msg": "Permission added"}

@router.post("/{user_id}/remove_permission")
async def remove_permission_from_user(
    user_id: int,
    permission_name: str = Body(..., embed=True),
    db: AsyncSession = Depends(get_db),
    perm: bool = Depends(require_permission("user:update"))
):
    result = await db.execute(select(Permission).where(Permission.name == permission_name))
    permission = result.scalar_one_or_none()
    if not permission:
        raise HTTPException(status_code=404, detail="Permission not found")
    result = await db.execute(
        select(UserPermission).where(
            UserPermission.user_id == user_id,
            UserPermission.permission_id == permission.id
        )
    )
    user_permission = result.scalar_one_or_none()
    if not user_permission:
        raise HTTPException(status_code=404, detail="User does not have this permission")
    await db.delete(user_permission)
    await db.commit()
    return {"msg": "Permission removed"}