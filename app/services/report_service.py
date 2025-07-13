import requests
import re
import random
from lxml import html
from datetime import datetime,timedelta
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import insert
from ..models.hoa_don_models import HoaDon
from ..models.hoa_don_momo_model import HoaDonDien
from ..schemas.report_schemas import HoaDonCalendarEvent
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
from sqlalchemy import select, func, cast, Date,Integer
from sqlalchemy.orm import aliased
from dateutil.relativedelta import relativedelta

async def report_summary(type, from_, to, db, current_user=User):
    # if current_user.role != UserRole.ADMIN:
    #     raise HTTPException(status_code=403, detail="No permission")

    # Chọn hàm group theo type
    if type == 'hour':
        group_expr = func.substr(hoa_don_models.HoaDon.gio_giao_dich, 1, 2)
    elif type == 'day':
        group_expr = cast(hoa_don_models.HoaDon.created_at, Date)
    elif type == 'week':
        group_expr = func.date_format(hoa_don_models.HoaDon.created_at, '%Y-%u')
    elif type == 'month':
        group_expr = func.date_format(hoa_don_models.HoaDon.created_at, '%Y-%m')
    elif type == 'year':
        group_expr = func.date_format(hoa_don_models.HoaDon.created_at, '%Y')
    else:
        raise HTTPException(status_code=400, detail="Invalid type")
    to_plus_1 = to + timedelta(days=1)

    # Truy vấn tất cả hóa đơn trong khoảng
    stmt = (
        select(
            hoa_don_models.HoaDon.batch_id,
            hoa_don_models.HoaDon.tong_so_tien,
            hoa_don_models.HoaDon.tien_phi,
            hoa_don_models.HoaDon.khach_moi,
            group_expr.label("period")
        )
        .where(
            hoa_don_models.HoaDon.created_at >= from_,
            hoa_don_models.HoaDon.created_at <= to_plus_1
        )
        .order_by("period")
    )

    result = await db.execute(stmt)
    records = result.fetchall()

    # Group dữ liệu theo period
    summary = {}
    for r in records:
        period = str(r.period)
        batch_id = r.batch_id
        tong_so_tien = r.tong_so_tien
        tien_phi = r.tien_phi
        khach_moi = r.khach_moi
        if period not in summary:
            summary[period] = {
                "total_amount": 0,
                "total_fee": 0,
                "total_batches": set(),
                "seen_batches": set(),
                "new_customers": 0
            }

        # tổng tiền
        if tong_so_tien and str(tong_so_tien).isdigit():
            summary[period]["total_amount"] += int(tong_so_tien)

        # đếm batch
        if batch_id:
            summary[period]["total_batches"].add(batch_id)

            # tính phí 1 lần/batch
            if batch_id not in summary[period]["seen_batches"]:
                summary[period]["seen_batches"].add(batch_id)
                if tien_phi and str(tien_phi).isdigit():
                    summary[period]["total_fee"] += int(tien_phi)
        if khach_moi and str(khach_moi).lower() in ['true', '1', 'yes']:
            summary[period]["new_customers"] += 1
    # Format output
    return [
        {
            "period": period,
            "total_amount": data["total_amount"],
            "total_fee": data["total_fee"],
            "total_batches": len(data["total_batches"]),
            "total_new_customers": data["new_customers"]
        }
        for period, data in sorted(summary.items())
    ]

async def commission_by_sender(from_date, to_date, db, current_user):
    # Query tổng hợp theo nguoi_gui
    stmt = (
        select(
            HoaDon.nguoi_gui,
            func.sum(HoaDon.tien_phi).label("total_fee"),
            func.sum(HoaDon.tong_so_tien).label("total_amount"),
            func.count(HoaDon.id).label("total_transactions"),
        )
        .where(HoaDon.ngay_giao_dich >= from_date)
        .where(HoaDon.ngay_giao_dich <= to_date)
        .group_by(HoaDon.nguoi_gui)
    )

    result = await db.execute(stmt)
    rows = result.all()

    # Xử lý kết quả
    response = []
    for row in rows:
        response.append({
            "nguoi_gui": row.nguoi_gui,
            "total_transactions": row.total_transactions or 0,
            "total_amount": row.total_amount or 0,
            "total_fee": row.total_fee or 0,
            "total_commission": (row.total_amount or 0) * 0.0002,
            "hoa_hong_cuoi_cung": (row.total_amount or 0) * 0.0002
        })


    return response

async def get_hoa_don_den_han_ket_toan(from_dt, to_dt, db, current_user):
    to_dt_safe = to_dt + timedelta(days=1)

    # Tạo window function: đánh số thứ tự trong mỗi batch
    row_number_expr = func.row_number().over(
        partition_by=HoaDon.batch_id,
        order_by=HoaDon.id.asc()
    ).label("rn")
    
    # Subquery: lấy tất cả hóa đơn + rn
    conditions = [
        HoaDon.lich_canh_bao_datetime >= from_dt,
        HoaDon.lich_canh_bao_datetime < to_dt_safe,
    ]

    if current_user.role == UserRole.USER:
        conditions.append(HoaDon.nguoi_gui == current_user.username)

    # Subquery: lấy tất cả hóa đơn + rn
    subq = (
        select(
            HoaDon.id,
            HoaDon.batch_id,
            HoaDon.ten_khach,
            HoaDon.nguoi_gui,
            HoaDon.so_dien_thoai,
            HoaDon.tong_so_tien,
            HoaDon.tien_phi,
            HoaDon.lich_canh_bao_datetime,
            HoaDon.so_hoa_don,
            HoaDon.tinh_trang,
            row_number_expr
        )
        .where(*conditions)
    ).subquery()

    HD = aliased(subq)

    # Truy vấn chính: chỉ lấy những hóa đơn đại diện mỗi batch
    stmt = (
        select(HD)
        .where(HD.c.rn == 1)
        .order_by(HD.c.lich_canh_bao_datetime)
    )

    result = await db.execute(stmt)
    hoa_dons = result.fetchall()

    # Format dữ liệu cho FullCalendar
    return [
        HoaDonCalendarEvent(
            id=row.id,
            title=f"{row.ten_khach or ''} - {row.so_dien_thoai or ''}",
            start=row.lich_canh_bao_datetime + relativedelta(months=1),
            ten_khach=row.ten_khach,
            nguoi_gui=row.nguoi_gui,
            so_dien_thoai=row.so_dien_thoai,
            batch_id=row.batch_id,
            thoi_gian= row.lich_canh_bao_datetime
        )
        for row in hoa_dons
    ]