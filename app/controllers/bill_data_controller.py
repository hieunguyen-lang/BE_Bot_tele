from fastapi import APIRouter, Depends, HTTPException, status,Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from ..database import get_db
from ..redis_client import get_redis
from ..models import User, UserRole
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from ..schemas.hoadon_dien_schemas import HoaDonDienCreate,HoaDonDienUpdate
from ..auth import get_current_active_user, get_current_admin_user
from ..services import bill_data
from typing import Any
from app.auth_permission import require_permission
router = APIRouter()



#Hóa đơn điện

@router.get("/stats-hoa-don-dien")
async def get_hoa_don_dien_stats(
    ma_giao_dich: str = Query(None),
    ten_zalo: str = Query(None),
    nguoi_gui: str = Query(None),
    ma_khach_hang: str = Query(None),
    from_date: str = Query(None, description="Format: YYYY-MM-DD"),
    to_date: str = Query(None, description="Format: YYYY-MM-DD"),
    search: str = Query(None, description="Tìm kiếm theo tên KH, mã KH, mã GD"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:view"))
):
    
    filters = {
        "ma_giao_dich": ma_giao_dich,
        "ten_zalo": ten_zalo,
        "ma_khach_hang": ma_khach_hang,
        "nguoi_gui": nguoi_gui,
        "from_date": from_date,
        "to_date": to_date,
        "search": search,
    }
    
    # Loại bỏ các giá trị None khỏi filters
    filters = {k: v for k, v in filters.items() if v is not None}
    
    return await bill_data.get_hoa_don_dien_stats(
        db=db, 
        filters=filters,
        current_user=current_user
    )

@router.get("/momo")
async def get_hoa_don_dien_grouped(
    page: int = Query(1, ge=1),
    page_size: int = Query(200, ge=1),
    ma_giao_dich: str = Query(None),
    ten_zalo: str = Query(None),
    nguoi_gui: str = Query(None),
    ma_khach_hang: str = Query(None),
    from_date: str = Query(None, description="Format: YYYY-MM-DD"),
    to_date: str = Query(None, description="Format: YYYY-MM-DD"),
    search: str = Query(None, description="Tìm kiếm theo tên KH, mã KH, mã GD"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:view"))
):
    
    filters = {
        "ma_giao_dich": ma_giao_dich,
        "ten_zalo": ten_zalo,
        "ma_khach_hang": ma_khach_hang,
        "nguoi_gui": nguoi_gui,
        "from_date": from_date,
        "to_date": to_date,
        "search": search,
    }
    
    # Loại bỏ các giá trị None khỏi filters
    filters = {k: v for k, v in filters.items() if v is not None}
    
    return await bill_data.get_hoa_don_dien_grouped(
        db=db, 
        page=page, 
        page_size=page_size, 
        filters=filters,
        current_user=current_user
    )

@router.post("/momo", status_code=201)
async def create_hoa_don_dien(
    hoa_don: HoaDonDienCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
    redis=Depends(get_redis),
    perm: bool = Depends(require_permission("bill:create"))
):
    
    return await bill_data.create_hoa_don_dien(db,hoa_don,redis)
    

@router.put("/momo/{id}")
async def update_hoa_don_dien(
    id: int,
    hoa_don: HoaDonDienUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
    redis=Depends(get_redis),
    perm: bool = Depends(require_permission("bill:update"))
):  
    return await bill_data.update_hoa_don_dien(db, hoa_don, id,redis)
    
@router.delete("/momo/{id}")
async def delete_hoa_don_dien(
    id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
    redis=Depends(get_redis),
    perm: bool = Depends(require_permission("bill:delete"))
):
    return await bill_data.delete_hoa_don_dien(db,id,redis)

@router.delete("/momo/batch/{batch_id}")
async def delete_hoa_don_dien_batch(
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
    redis=Depends(get_redis),
    perm: bool = Depends(require_permission("bill:delete"))
):
    return await bill_data.delete_hoa_don_dien_batch(db,batch_id,redis)
#http://localhost:8000/hoa-don/momo/batch/6e90e160-3a5d-4228-8e3a-42468fa25c13


#Hóa đơn đối ứng
@router.get("/doi-ung")
async def get_doi_ung_flat(
    page: int = Query(1, ge=1),
    page_size: int = Query(200, ge=1),
    ma_giao_dich: str = Query(None),
    nguoi_gui: str = Query(None),
    ma_khach_hang: str = Query(None),
    ten_khach_hang: str = Query(None),
    from_date: str = Query(None, description="Format: YYYY-MM-DD"),
    to_date: str = Query(None, description="Format: YYYY-MM-DD"),
    search: str = Query(None, description="Tìm kiếm theo tên KH, mã KH, mã GD"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:view"))
):
    
    filters = {
        "ma_giao_dich": ma_giao_dich,
        "ma_khach_hang": ma_khach_hang,
        "ten_khach_hang": ten_khach_hang,
        "nguoi_gui": nguoi_gui,
        "from_date": from_date,
        "to_date": to_date,
        "search": search,
    }
    
    # Loại bỏ các giá trị None khỏi filters
    filters = {k: v for k, v in filters.items() if v is not None}
    
    return await bill_data.get_doi_ung_flat(
        db=db, 
        page=page, 
        page_size=page_size, 
        filters=filters,
        current_user=current_user
    )

@router.get("/stats-doi-ung")
async def get_doi_ung_stats(
    ma_giao_dich: str = Query(None),
    nguoi_gui: str = Query(None),
    ma_khach_hang: str = Query(None),
    ten_khach_hang: str = Query(None),
    from_date: str = Query(None, description="Format: YYYY-MM-DD"),
    to_date: str = Query(None, description="Format: YYYY-MM-DD"),
    search: str = Query(None, description="Tìm kiếm theo tên KH, mã KH, mã GD"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:view"))
):
    
    filters = {
        "ma_giao_dich": ma_giao_dich,
        "ma_khach_hang": ma_khach_hang,
        "ten_khach_hang": ten_khach_hang,
        "nguoi_gui": nguoi_gui,
        "from_date": from_date,
        "to_date": to_date,
        "search": search,
    }
    
    # Loại bỏ các giá trị None khỏi filters
    filters = {k: v for k, v in filters.items() if v is not None}
    
    return await bill_data.get_doi_ung_stats(
        db=db, 
        filters=filters,
        current_user=current_user
    )



#hóa đơn đáo rút
@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_active_user), perm: bool = Depends(require_permission("bill:view"))):
    return await bill_data.get_hoa_don_stats(db, current_user)

@router.get("/stats-hoadon")
async def get_stats(
    so_hoa_don: str = Query(None),
    so_lo: str = Query(None),
    tid: str = Query(None),
    mid: str = Query(None),
    nguoi_gui: str = Query(None),
    ten_khach: str = Query(None),
    so_dien_thoai: str = Query(None),
    ngay_giao_dich: str = Query(None),
    db: AsyncSession = Depends(get_db), 
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:view"))
    ):
    return await bill_data.get_hoa_don_stats_hoa_don(
        so_hoa_don,
        so_lo,
        tid,
        mid,
        nguoi_gui,
        ten_khach,
        so_dien_thoai,
        ngay_giao_dich,
        db, 
        current_user
        )

@router.get("/")
async def get_hoa_don_grouped(
    page: int = Query(1, ge=1),
    page_size: int = Query(200, ge=1),
    so_hoa_don: str = Query(None),
    so_lo: str = Query(None),
    tid: str = Query(None),
    mid: str = Query(None),
    nguoi_gui: str = Query(None),
    ten_khach: str = Query(None),
    so_dien_thoai: str = Query(None),
    ngay_giao_dich: str = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:view"))
):
    
    filters = {
        "so_hoa_don": so_hoa_don,
        "so_lo": so_lo,
        "tid": tid,
        "mid": mid,
        "nguoi_gui": nguoi_gui,
        "ten_khach": ten_khach,
        "so_dien_thoai": so_dien_thoai,
        "ngay_giao_dich": ngay_giao_dich,
    }
    
    return await bill_data.get_hoa_don_grouped(
        db=db, 
        page=page, 
        page_size=page_size, 
        filters=filters,
        current_user=current_user
    )

@router.post("/", response_model=HoaDonOut)
async def create_hoa_don(
    hoa_don: HoaDonCreate, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:create")),
    redis=Depends(get_redis)
    ):
    return await bill_data.create_hoa_don(
        db=db, 
        hoa_don=hoa_don,
        current_user=current_user,
        redis=redis
    )

@router.put("/{hoa_don_id}", response_model=HoaDonOut)
async def update_hoa_don(
    hoa_don_id: int, 
    hoa_don: HoaDonUpdate, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:update")),
    redis=Depends(get_redis)
):
    return await bill_data.update_hoa_don(
            hoa_don_id, 
            hoa_don,
            db,
            current_user=current_user,
            redis=redis
        )

@router.delete("/{hoa_don_id}")
async def delete_hoa_don(
    hoa_don_id: int, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:delete")),
    redis=Depends(get_redis)
):
    return await bill_data.delete_hoa_don(
            hoa_don_id, 
            db,
            current_user,
            redis
    )

@router.delete("/batch/{batch_id}")
async def delete_hoa_don_batch_id(
    batch_id: str, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:delete")),
    redis=Depends(get_redis)
):
    return await bill_data.delete_hoa_don_batch_id(
            batch_id, 
            db,
            current_user,
            redis
    )



@router.get("/export-excel")
async def export_hoa_don_excel(
    page: int = Query(1, ge=1),
    page_size: int = Query(200, ge=1),
    so_hoa_don: str = Query(None),
    so_lo: str = Query(None),
    tid: str = Query(None),
    mid: str = Query(None),
    nguoi_gui: str = Query(None),
    ten_khach: str = Query(None),
    so_dien_thoai: str = Query(None),
    ngay_giao_dich: str = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    perm: bool = Depends(require_permission("bill:export"))
): 
    filters = {
        "so_hoa_don": so_hoa_don,
        "so_lo": so_lo,
        "tid": tid,
        "mid": mid,
        "nguoi_gui": nguoi_gui,
        "ten_khach": ten_khach,
        "so_dien_thoai": so_dien_thoai,
        "ngay_giao_dich": ngay_giao_dich,
    }
    return await bill_data.export_hoa_don_excel(
        page=page, 
        page_size=page_size, 
        db=db, 
        filters=filters,
        current_user=current_user
    )
router = router