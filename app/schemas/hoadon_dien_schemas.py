from typing import Optional, List
from .base import BaseSchema, TimestampSchema
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, validator, Field
import re
import uuid
class HoaDonDienBase(BaseSchema):
    nha_cung_cap: Optional[str] = None
    ten_khach_hang: Optional[str] = None
    ma_khach_hang: Optional[str] = None
    dia_chi: Optional[str] = None
    ky_thanh_toan: Optional[str] = None
    so_tien: Optional[str] = None
    ma_giao_dich: Optional[str] = None
    thoi_gian: Optional[datetime] = None
    tai_khoan_the: Optional[str] = None
    tong_phi: Optional[str] = None
    trang_thai: Optional[str] = None
    nguoi_gui : Optional[str] = None
    batch_id: Optional[str] = None
    update_at: Optional[datetime] = None
    ten_zalo: Optional[str] = None
    phi_cong_ty_thu:Optional[str] = None
    key_redis:Optional[str] = None
    ck_vao: Optional[str]
    ck_ra: Optional[str]
    ma_chuyen_khoan: Optional[str]
    so_tk: Optional[str]
    is_send_or_recieve: Optional[str]
    

class HoaDonDienOut(HoaDonDienBase):
    id: int
    class Config:
        orm_mode = True      

class HoaDonDienCreate(BaseModel):
    nha_cung_cap: Optional[str]
    ten_khach_hang: Optional[str]
    ma_khach_hang: Optional[str]
    dia_chi: Optional[str]
    ky_thanh_toan: Optional[str]
    so_tien: Optional[int]
    ma_giao_dich: Optional[str]
    thoi_gian: Optional[datetime] 
    tai_khoan_the: Optional[str]
    tong_phi: Optional[str]
    trang_thai: Optional[str]
    nguoi_gui: Optional[str]
    batch_id: Optional[str]
    update_at: Optional[datetime] 
    ten_zalo: Optional[str]
    key_redis: Optional[str]
    phi_cong_ty_thu:Optional[str]
    ck_vao: Optional[str]
    ck_ra: Optional[str]
    ma_chuyen_khoan: Optional[str]
    so_tk: Optional[str]
    is_send_or_recieve: Optional[str]
class HoaDonDienUpdate(HoaDonDienCreate):
    pass