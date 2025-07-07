from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models.user import User
from app.services.permission_service import get_user_permissions
from app.auth import get_current_user

def require_permission(permission_name: str):
    async def dependency(
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db),
    ):
        permissions = await get_user_permissions(db, current_user.id)
        if permission_name not in permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: missing permission"
            )
        return True
    return dependency 