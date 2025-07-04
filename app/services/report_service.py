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
from sqlalchemy import select, func, cast, Date,Integer
async def report_summary(type, from_, to, db, current_user=User):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="No permission")

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

        if period not in summary:
            summary[period] = {
                "total_amount": 0,
                "total_fee": 0,
                "total_batches": set(),
                "seen_batches": set()
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

    # Format output
    return [
        {
            "period": period,
            "total_amount": data["total_amount"],
            "total_fee": data["total_fee"],
            "total_batches": len(data["total_batches"])
        }
        for period, data in sorted(summary.items())
    ]

async def commission_by_sender(from_date, to_date, db, current_user):
    from sqlalchemy.orm import aliased
    

    # Tạo window function: đánh số thứ tự mỗi hóa đơn theo batch_id
    row_number_expr = func.row_number().over(
        partition_by=HoaDon.batch_id,
        order_by=HoaDon.id.asc()
    ).label("rn")

    # Subquery chọn mỗi batch_id một hóa đơn duy nhất
    subq = (
        select(
            HoaDon.id,
            HoaDon.nguoi_gui,
            HoaDon.tien_phi,
            HoaDon.tong_so_tien,
            HoaDon.ngay_giao_dich,
            HoaDon.batch_id,
            row_number_expr
        )
        .where(HoaDon.ngay_giao_dich >= from_date)
        .where(HoaDon.ngay_giao_dich <= to_date)
    ).subquery()

    # Aliased cho dễ xử lý
    HD = aliased(subq)

    # Truy vấn chính: chỉ lấy những dòng có rn == 1 (tức là duy nhất mỗi batch_id)
    stmt = (
        select(
            HD.c.nguoi_gui,
            func.sum(HD.c.tien_phi).label("total_fee"),
            func.sum(HD.c.tong_so_tien).label("total_amount"),
            func.sum(HD.c.tien_phi).label("total_commission"),
            func.count(HD.c.id).label("total_transactions"),
        )
        .where(HD.c.rn == 1)
        .group_by(HD.c.nguoi_gui)
    )

    result = await db.execute(stmt)
    rows = result.all()

    # Xử lý kết quả
    response = []
    for row in rows:
        hoa_hong_cuoi_cung = (row.total_fee or 0) * 0.02
        response.append({
            "nguoi_gui": row.nguoi_gui,
            "total_commission": row.total_commission or 0,
            "total_transactions": row.total_transactions or 0,
            "total_amount": row.total_amount or 0,
            "total_fee": row.total_fee or 0,
            "hoa_hong_cuoi_cung": hoa_hong_cuoi_cung
        })

    return response