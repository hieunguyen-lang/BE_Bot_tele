from fastapi import APIRouter, Depends, HTTPException, status,Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from ..database import get_db
from ..redis_client import get_redis
from ..models import User, UserRole
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from ..schemas.hoadon_dien_schemas import HoaDonDienCreate,HoaDonDienUpdate
from ..schemas.search_schemas import CrawlerPostItem
from ..auth import get_current_active_user, get_current_admin_user
from ..services import search_service
from typing import Any
from app.auth_permission import require_permission
router = APIRouter()



#Hóa đơn điện

@router.get("/",response_model=List[CrawlerPostItem], summary="Tìm kiếm bài viết trên mạng xã hội")
async def searchSocial(
    keyword: str = Query(None, description="Tìm kiếm keyword"),

    # current_user: User = Depends(get_current_active_user),
    # perm: bool = Depends(require_permission("bill:view"))
):
    filters = {
        "keyword": keyword,
    }
    # Loại bỏ các giá trị None khỏi filters
    filters = {k: v for k, v in filters.items() if v is not None}
    return await search_service.searchSocial(
        filters=filters,
    )


router = router