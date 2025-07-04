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
            hoa_don_models.HoaDon.created_at <= to
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


