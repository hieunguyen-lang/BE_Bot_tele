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
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from .. schemas.hoadon_dien_schemas import HoaDonDienOut
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

async def get_hoa_don_stats_hoa_don(so_hoa_don,so_lo,tid,mid,nguoi_gui,ten_khach,so_dien_thoai,ngay_giao_dich,db, current_user):
    

    # Tạo base query
    query = select(
        hoa_don_models.HoaDon.batch_id,
        hoa_don_models.HoaDon.tong_so_tien,
        hoa_don_models.HoaDon.tien_phi
    )
    if current_user.role != UserRole.ADMIN:
        query = query.where(hoa_don_models.HoaDon.nguoi_gui == current_user.username)
    # Áp dụng filter nếu có
    if so_hoa_don:
        query = query.where(hoa_don_models.HoaDon.so_hoa_don.contains(so_hoa_don))
    if so_lo:
        query = query.where(hoa_don_models.HoaDon.so_lo.contains(so_lo))
    if tid:
        query = query.where(hoa_don_models.HoaDon.tid.contains(tid))
    if mid:
        query = query.where(hoa_don_models.HoaDon.mid.contains(mid))
    if nguoi_gui:
        query = query.where(hoa_don_models.HoaDon.nguoi_gui.contains(nguoi_gui))
    if ten_khach:
        query = query.where(hoa_don_models.HoaDon.ten_khach.contains(ten_khach))
    if so_dien_thoai:
        query = query.where(hoa_don_models.HoaDon.so_dien_thoai.contains(so_dien_thoai))
    if ngay_giao_dich:
        query = query.where(hoa_don_models.HoaDon.ngay_giao_dich == ngay_giao_dich)

    result = await db.execute(query)
    records = result.fetchall()

    # Tính toán thống kê như cũ
    total_records = len(records)
    total_batches = len(set(r[0] for r in records if r[0]))  # batch_id
    total_amount = sum(int(r[1]) for r in records if r[1] and str(r[1]).isdigit())  # tong_so_tien
    seen_batches = set()
    total_fee = 0
    for r in records:
        batch_id = r[0]
        tien_phi = r[2]
        if batch_id and batch_id not in seen_batches:
            seen_batches.add(batch_id)
            if tien_phi and str(tien_phi).isdigit():
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


async def get_hoa_don_dien_grouped(page, page_size, db, filters=None,current_user=User):
    # if current_user.role != UserRole.ADMIN or current_user.role != UserRole.USER:
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail="You do not have permission to perform this action."
    #     )
    # Tạo base query với filters
    base_query = select(HoaDonDien)
    # 2. Nếu không phải admin → chỉ được xem hóa đơn của mình
    if current_user.role != UserRole.ADMIN:
        base_query = base_query.where(HoaDonDien.nguoi_gui == current_user.username)
    # Áp dụng filters
    if filters:
        if filters.get("ma_giao_dich"):
            base_query = base_query.where(HoaDonDien.ma_giao_dich.contains(filters["ma_giao_dich"]))
        if filters.get("ten_zalo"):
            base_query = base_query.where(HoaDonDien.ten_zalo(filters["ten_zalo"]))
        if filters.get("nguoi_gui"):
            base_query = base_query.where(HoaDonDien.nguoi_gui.contains(filters["nguoi_gui"]))
        # Thêm filter theo thời gian
        if filters.get("from_date"):
            from_date = datetime.strptime(filters["from_date"], "%Y-%m-%d").date()
            base_query = base_query.where(HoaDonDien.thoi_gian >= from_date)
        
        if filters.get("to_date"):
            to_date = datetime.strptime(filters["to_date"], "%Y-%m-%d").date()
            # Thêm 1 ngày để bao gồm cả ngày kết thúc
            to_date = to_date + timedelta(days=1)
            base_query = base_query.where(HoaDonDien.thoi_gian < to_date)

    # 1. Lấy danh sách batch_id (phân trang) với filter
    sub = (
        select(
            HoaDonDien.batch_id,
            func.min(HoaDonDien.thoi_gian).label("min_time")
        )
        .where(*base_query._where_criteria)  # sử dụng filter có sẵn
        .group_by(HoaDonDien.batch_id)
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
            select(HoaDonDien.batch_id)
            .where(*base_query._where_criteria)
            .distinct()
            .subquery()
        )
    )
    total_result = await db.execute(stmt_total)
    total = total_result.scalar()

    # 4. Lấy record theo batch_id
    stmt_records = base_query.where(HoaDonDien.batch_id.in_(batch_ids))
    result = await db.execute(stmt_records)
    records = result.scalars().all()

    # 5. Nhóm lại
    grouped = defaultdict(list)
    for r in records:
        masked_so_the = None

        hoa_don_dict = r.__dict__.copy()
        hoa_don_dict["so_the"] = masked_so_the
        grouped[r.batch_id].append(HoaDonDienOut(**hoa_don_dict))

    data = [
        {"batch_id": batch_id, "records": grouped[batch_id]}
        for batch_id in batch_ids
    ]

    return {"total": total, "data": data}

async def create_hoa_don(db, hoa_don, current_user=User):
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action."
        )
    
    # Validation các field
    validation_errors = []
    
    # 1. Validate ngày giao dịch
    if hoa_don.ngay_giao_dich:
        try:
            datetime.strptime(hoa_don.ngay_giao_dich, '%Y-%m-%d')
        except ValueError:
            validation_errors.append("Ngày giao dịch không đúng định dạng YYYY-MM-DD")
    
    # 2. Validate giờ giao dịch
    if hoa_don.gio_giao_dich:
        if not re.match(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$', hoa_don.gio_giao_dich):
            validation_errors.append("Giờ giao dịch không đúng định dạng HH:MM")
    
    # 3. Validate số tiền
    if hoa_don.tong_so_tien:
        try:
            amount = int(hoa_don.tong_so_tien)
            if amount <= 0:
                validation_errors.append("Tổng số tiền phải lớn hơn 0")
        except ValueError:
            validation_errors.append("Tổng số tiền phải là số nguyên")
    
    # 4. Validate phí
    if hoa_don.tien_phi:
        try:
            fee = int(hoa_don.tien_phi)
            if fee < 0:
                validation_errors.append("Phí không được âm")
        except ValueError:
            validation_errors.append("Phí phải là số nguyên")
    
    # 5. Validate CK vào/ra
    if hoa_don.ck_vao:
        try:
            ck_vao = int(hoa_don.ck_vao)
            if ck_vao < 0:
                validation_errors.append("CK vào không được âm")
        except ValueError:
            validation_errors.append("CK vào phải là số nguyên")
    
    if hoa_don.ck_ra:
        try:
            ck_ra = int(hoa_don.ck_ra)
            if ck_ra < 0:
                validation_errors.append("CK ra không được âm")
        except ValueError:
            validation_errors.append("CK ra phải là số nguyên")
    
    # 6. Validate số điện thoại
    if hoa_don.so_dien_thoai:
        if not re.match(r'^[0-9]{10,11}$', hoa_don.so_dien_thoai):
            validation_errors.append("Số điện thoại không hợp lệ (10-11 số)")
    
    # 7. Validate số thẻ (nếu có)
    if hoa_don.so_the:
        if not re.match(r'^[0-9]{4,19}$', hoa_don.so_the):
            validation_errors.append("Số thẻ không hợp lệ")
    
    # 8. Validate TID/MID
    if hoa_don.tid and len(hoa_don.tid) > 50:
        validation_errors.append("TID quá dài (tối đa 50 ký tự)")
    
    if hoa_don.mid and len(hoa_don.mid) > 50:
        validation_errors.append("MID quá dài (tối đa 50 ký tự)")
    
    # 9. Validate tên khách
    if hoa_don.ten_khach and len(hoa_don.ten_khach.strip()) == 0:
        validation_errors.append("Tên khách không được để trống")
    
    # 10. Validate người gửi
    if hoa_don.nguoi_gui and len(hoa_don.nguoi_gui.strip()) == 0:
        validation_errors.append("Người gửi không được để trống")
    
    # Nếu có lỗi validation, trả về tất cả lỗi
    if validation_errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": "Validation failed", "errors": validation_errors}
        )
    
    # Tạo hóa đơn mới
    try:
        db_hoa_don = HoaDon(**hoa_don.dict())
        db.add(db_hoa_don)
        await db.commit()
        await db.refresh(db_hoa_don)
        return db_hoa_don
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Lỗi khi tạo hóa đơn (có thể trùng lặp dữ liệu)"
        )
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Lỗi server khi tạo hóa đơn"
        )

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
                "ngay_giao_dich": r.ngay_giao_dich,
                "nguoi_gui": r.nguoi_gui,
                "ten_khach": r.ten_khach,
                "so_dien_thoai": r.so_dien_thoai,
                "type_dao_rut": r.type_dao_rut,
                "ket_toan": tong_cong,
                "so_the": r.so_the,
                "tid": r.tid,
                "so_lo": r.so_lo,
                "so_hoa_don": r.so_hoa_don,
                "gio_giao_dich": r.gio_giao_dich,
                "ten_may_pos": r.ten_may_pos,
                "tong_so_tien": r.tong_so_tien,
                "phan_tram_phi": r.phan_tram_phi,
                "tien_phi": r.tien_phi,
                "ck_ra": r.ck_ra,
                "ck_vao": r.ck_vao,
                "stk_khach": r.stk_khach,
                "stk_cty": r.stk_cty,
                "dia_chi": r.dia_chi,
                "caption_goc": r.caption_goc,
                "ly_do": r.ly_do,
                "tinh_trang": r.tinh_trang
            }

            all_rows.append(row)

    # ✅ Tạo Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "Hóa đơn"

    headers = [
        "STT",
        "NGÀY",
        "NGƯỜI GỬI",
        "TÊN KHÁCH",
        "SĐT KHÁCH",
        "ĐÁO / RÚT",
        "KẾT TOÁN",
        "SỐ THẺ",
        "TID",
        "SỐ LÔ",
        "SỐ HÓA ĐƠN",
        "GIỜ GIAO DỊCH",
        "POS",
        "SỐ TIỀN",
        "Phí %",
        "PHÍ DV",
        "CK ra",
        "CK vào",
        "STK KHÁCH",
        "STK CÔNG TY",
        "ĐỊA CHỈ",
        "NOTE GỐC",
        "LÝ DO",
        "TÌNH TRẠNG",
    ]

    ws.append(headers)

    for idx, r in enumerate(all_rows, 1):
        ws.append([
            idx,
            r.get("ngay"),
            r.get("nguoi_gui"),
            r.get("ten_khach"),
            r.get("sdt_khach"),
            r.get("loai"),
            r.get("ket_toan"),
            r.get("so_the"),
            r.get("tid"),
            r.get("so_lo"),
            r.get("so_hoa_don"),
            r.get("gio"),
            r.get("ten_pos"),
            r.get("so_tien"),
            r.get("phan_tram_phi"),
            r.get("phi_dv"),
            r.get("ck_ra"),
            r.get("ck_vao"),
            r.get("stk_khach"),
            r.get("stk_cty"),
            r.get("dia_chi"),
            r.get("caption_goc"),
            r.get("ly_do"),
            r.get("tinh_trang"),
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
