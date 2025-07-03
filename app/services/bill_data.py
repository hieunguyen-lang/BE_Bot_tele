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
from fastapi.responses import StreamingResponse
from collections import defaultdict
from typing import List, Dict
from sqlalchemy import asc ,desc
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
async def get_hoa_don_stats(db, current_user=User):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action."
        )
    
    # Chỉ select các trường cần thiết cho thống kê
    stmt = select(
        hoa_don_models.HoaDon.batch_id,
        hoa_don_models.HoaDon.tong_so_tien,
        hoa_don_models.HoaDon.tien_phi
    )
    
    result = await db.execute(stmt)
    records = result.fetchall()
    
    # Tính toán thống kê
    total_records = len(records)
    total_batches = len(set(r[0] for r in records if r[0]))  # batch_id
    total_amount = sum(int(r[1]) for r in records if r[1] and r[1].isdigit())  # tong_so_tien
    # ✅ Fix: chỉ tính tien_phi của mỗi batch 1 lần (batch đầu tiên)
    seen_batches = set()
    total_fee = 0
    for r in records:
        batch_id = r[0]
        tien_phi = r[2]
        if batch_id and batch_id not in seen_batches:
            seen_batches.add(batch_id)
            if tien_phi and tien_phi.isdigit():
                total_fee += int(tien_phi)
    return {
        "totalRecords": total_records,
        "totalBatches": total_batches,
        "totalAmount": total_amount,
        "totalFee": total_fee
    }

async def get_hoa_don_grouped(page, page_size, db, filters=None,current_user=User):
    # if current_user.role != UserRole.ADMIN or current_user.role != UserRole.USER:
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail="You do not have permission to perform this action."
    #     )
    # Tạo base query với filters
    base_query = select(hoa_don_models.HoaDon)
    # 2. Nếu không phải admin → chỉ được xem hóa đơn của mình
    if current_user.role != UserRole.ADMIN:
        base_query = base_query.where(hoa_don_models.HoaDon.nguoi_gui == current_user.username)
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
        masked_so_the = None
        if r.so_the and len(r.so_the) >= 4:
            masked_so_the = "*" * (len(r.so_the) - 4) + r.so_the[-4:]

        hoa_don_dict = r.__dict__.copy()
        hoa_don_dict["so_the"] = masked_so_the
        grouped[r.batch_id].append(HoaDonOut(**hoa_don_dict))

    data = [
        {"batch_id": batch_id, "records": grouped[batch_id]}
        for batch_id in batch_ids
    ]

    return {"total": total, "data": data}

async def create_hoa_don(db, hoa_don,current_user=User):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action."
        )
    db_hoa_don = HoaDon(**hoa_don.dict())
    db.add(db_hoa_don)
    db.commit()
    db.refresh(db_hoa_don)
    return db_hoa_don

async def update_hoa_don(
    hoa_don_id, 
    hoa_don,
    db,
    current_user
):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action."
        )
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
    db,
    current_user
):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action."
        )
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

async def export_hoa_don_excel(
   page, page_size, db, filters=None,current_user=User     
):
    data = await get_hoa_don_grouped(page, page_size, db, filters,current_user)
    all_rows = []
    for group in data["data"]:
        records = group["records"]
        # 👉 Merge KẾT TOÁN từ tổng các tong_so_tien
        tong_cong = sum(int(r.tong_so_tien or 0) for r in records)
        for r in records:
            row = {
                "ngay": r.ngay_giao_dich,
                "nguoi_gui": r.nguoi_gui,
                "ten_khach": r.ten_khach,
                "sdt_khach": r.so_dien_thoai,
                "loai": r.type_dao_rut,
                "so_tien": r.tong_so_tien,
                "ket_toan": tong_cong,
                "so_the": r.so_the,
                "tid": r.tid,
                "so_lo": r.so_lo,
                "so_hoa_don": r.so_hoa_don,
                "gio": r.gio_giao_dich,
                "ten_pos": r.ten_may_pos,
                "phi_pos": r.phi_pos,
                "phi_dv": r.tien_phi,
                "phi_thu_khach": r.phi_thu_khach,
                "ck_khach_rut": r.ck_khach_rut,
                "tien_ve_tk": r.tien_ve_tk_cty,
                "tinh_trang": r.tinh_trang,
                "ly_do": r.ly_do
            }
            all_rows.append(row)

    # ✅ Tạo Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "Hóa đơn"

    headers = [
        "STT", "NGÀY", "NGƯỜI GỬI", "HỌ VÀ TÊN KHÁCH", "SĐT KHÁCH", "ĐÁO / RÚT", "SỐ TIỀN", "KẾT TOÁN",
        "SỐ THẺ", "TID", "SỐ LÔ", "SỐ HÓA ĐƠN", "GIỜ GIAO DỊCH", "TÊN POS", "PHÍ POS", "PHÍ DV",
        "PHÍ THU KHÁCH", "CK KHÁCH RÚT", "TIỀN VỀ TK CTY", "TÌNH TRẠNG", "LÝ DO"
    ]
    ws.append(headers)

    for idx, r in enumerate(all_rows, 1):
        ws.append([
            idx,
            r["ngay"], r["nguoi_gui"], r["ten_khach"], r["sdt_khach"], r["loai"],
            r["so_tien"], r["ket_toan"], r["so_the"], r["tid"], r["so_lo"], r["so_hoa_don"],
            r["gio"], r["ten_pos"], r["phi_pos"], r["phi_dv"], r["phi_thu_khach"],
            r["ck_khach_rut"], r["tien_ve_tk"], r["tinh_trang"], r["ly_do"]
        ])

    # Co giãn cột
    for col in ws.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        ws.column_dimensions[get_column_letter(col[0].column)].width = max_len + 2

    file_stream = BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)

    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=hoa_don.xlsx"}
    )
