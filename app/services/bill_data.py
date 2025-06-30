import requests
import re
import random
from lxml import html
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import insert
from ..models.hoa_don_models import HoaDon
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from ..models import User, UserRole, hoa_don_models
from ..auth import verify_password, get_password_hash
from fastapi import HTTPException, status
from collections import defaultdict
from typing import List, Dict
from sqlalchemy import asc ,desc


async def get_hoa_don_grouped(page, page_size, db, filters=None):
    # Tạo base query với filters
    base_query = select(hoa_don_models.HoaDon)
    
    # Áp dụng filters
    if filters:
        if filters.get("so_hoa_don"):
            base_query = base_query.where(hoa_don_models.HoaDon.so_hoa_don.contains(filters["so_hoa_don"]))
        if filters.get("so_lo"):
            base_query = base_query.where(hoa_don_models.HoaDon.so_lo.contains(filters["so_lo"]))
        if filters.get("tid"):
            base_query = base_query.where(hoa_don_models.HoaDon.tid.contains(filters["tid"]))
        if filters.get("mid"):
            base_query = base_query.where(hoa_don_models.HoaDon.mid.contains(filters["mid"]))
        if filters.get("nguoi_gui"):
            base_query = base_query.where(hoa_don_models.HoaDon.nguoi_gui.contains(filters["nguoi_gui"]))
        if filters.get("ten_khach"):
            base_query = base_query.where(hoa_don_models.HoaDon.ten_khach.contains(filters["ten_khach"]))
        if filters.get("so_dien_thoai"):
            base_query = base_query.where(hoa_don_models.HoaDon.so_dien_thoai.contains(filters["so_dien_thoai"]))
        if filters.get("ngay_giao_dich"):
            base_query = base_query.where(hoa_don_models.HoaDon.ngay_giao_dich == filters["ngay_giao_dich"])

    # 1. Lấy danh sách batch_id (phân trang) với filter
    sub = (
        select(
            hoa_don_models.HoaDon.batch_id,
            func.min(hoa_don_models.HoaDon.thoi_gian).label("min_time")
        )
        .where(*base_query._where_criteria)  # sử dụng filter có sẵn
        .group_by(hoa_don_models.HoaDon.batch_id)
        .order_by(desc("min_time"))
        .offset((page - 1) * page_size)
        .limit(page_size)
        .subquery()
    )

    # 2. Lấy batch_id từ subquery
    stmt_batch_ids = select(sub.c.batch_id)
    result = await db.execute(stmt_batch_ids)
    batch_ids = [row[0] for row in result.fetchall()]

    # 3. Tổng số batch_id (không cần offset/limit)
    stmt_total = (
        select(func.count())
        .select_from(
            select(hoa_don_models.HoaDon.batch_id)
            .where(*base_query._where_criteria)
            .distinct()
            .subquery()
        )
    )
    total_result = await db.execute(stmt_total)
    total = total_result.scalar()

    # 4. Lấy record theo batch_id
    stmt_records = base_query.where(hoa_don_models.HoaDon.batch_id.in_(batch_ids))
    result = await db.execute(stmt_records)
    records = result.scalars().all()

    # 5. Nhóm lại
    grouped = defaultdict(list)
    for r in records:
        grouped[r.batch_id].append(HoaDonOut.from_orm(r))

    data = [
        {"batch_id": batch_id, "records": grouped[batch_id]}
        for batch_id in batch_ids
    ]

    return {"total": total, "data": data}

async def create_hoa_don(db, hoa_don):
    db_hoa_don = HoaDon(**hoa_don.dict())
    db.add(db_hoa_don)
    db.commit()
    db.refresh(db_hoa_don)
    return db_hoa_don

async def update_hoa_don(
    hoa_don_id, 
    hoa_don,
    db
):
    # Lấy hóa đơn
    stmt = select(HoaDon).where(HoaDon.id == hoa_don_id)
    result = await db.execute(stmt)
    db_hoa_don = result.scalar_one_or_none()
    
    if not db_hoa_don:
        raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")
    
    # Cập nhật fields
    update_data = hoa_don.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_hoa_don, field, value)
    
    await db.commit()
    await db.refresh(db_hoa_don)
    
    return db_hoa_don

async def delete_hoa_don(
    hoa_don_id: int, 
    db
):
    # Lấy hóa đơn
    stmt = select(HoaDon).where(HoaDon.id == hoa_don_id)
    result = await db.execute(stmt)
    db_hoa_don = result.scalar_one_or_none()
    
    if not db_hoa_don:
        raise HTTPException(status_code=404, detail="Hóa đơn không tồn tại")
    
    # Xóa hóa đơn
    await db.delete(db_hoa_don)
    await db.commit()
    
    return {"ok": True}