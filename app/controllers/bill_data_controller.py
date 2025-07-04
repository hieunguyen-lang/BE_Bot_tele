from fastapi import APIRouter, Depends, HTTPException, status,Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from ..database import get_db
from ..models import User, UserRole
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from ..auth import get_current_active_user, get_current_admin_user
from ..services import bill_data
from typing import Any
router = APIRouter()

@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_active_user)):
    return await bill_data.get_hoa_don_stats(db, current_user)

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
    current_user: User = Depends(get_current_active_user)
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
    current_user: User = Depends(get_current_active_user)
    ):
    return await bill_data.create_hoa_don(
        db=db, 
        hoa_don=hoa_don,
        current_user=current_user
    )

@router.put("/{hoa_don_id}", response_model=HoaDonOut)
async def update_hoa_don(
    hoa_don_id: int, 
    hoa_don: HoaDonUpdate, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    return await bill_data.update_hoa_don(
            hoa_don_id, 
            hoa_don,
            db,
            current_user=current_user
        )

@router.delete("/{hoa_don_id}")
async def delete_hoa_don(
    hoa_don_id: int, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    return await bill_data.delete_hoa_don(
            hoa_don_id, 
            db,
            current_user=current_user
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
    current_user: User = Depends(get_current_active_user)
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
        db=db, 
        page=page, 
        page_size=page_size, 
        filters=filters,
        current_user=current_user
    )