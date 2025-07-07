from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.permission import Permission
from app.models.role import Role
from app.models.role_permission import RolePermission
from app.models.user_permission import UserPermission
from app.models.user import User

# Lấy tất cả permission của user (bao gồm từ role và user-specific)
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

# Gán permission cho user
async def grant_permission_to_user(db: AsyncSession, user_id: int, permission_id: int):
    up = UserPermission(user_id=user_id, permission_id=permission_id)
    db.add(up)
    await db.commit()
    await db.refresh(up)
    return up

# Thu hồi permission khỏi user
async def revoke_permission_from_user(db: AsyncSession, user_id: int, permission_id: int):
    result = await db.execute(
        select(UserPermission).where(UserPermission.user_id == user_id, UserPermission.permission_id == permission_id)
    )
    up = result.scalar_one_or_none()
    if up:
        await db.delete(up)
        await db.commit()
    return up

# Gán permission cho role
async def grant_permission_to_role(db: AsyncSession, role_id: int, permission_id: int):
    rp = RolePermission(role_id=role_id, permission_id=permission_id)
    db.add(rp)
    await db.commit()
    await db.refresh(rp)
    return rp

# Thu hồi permission khỏi role
async def revoke_permission_from_role(db: AsyncSession, role_id: int, permission_id: int):
    result = await db.execute(
        select(RolePermission).where(RolePermission.role_id == role_id, RolePermission.permission_id == permission_id)
    )
    rp = result.scalar_one_or_none()
    if rp:
        await db.delete(rp)
        await db.commit()
    return rp 