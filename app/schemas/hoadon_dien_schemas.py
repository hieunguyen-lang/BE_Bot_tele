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

    

class HoaDonDienOut(HoaDonDienBase):
    id: int
    class Config:
        orm_mode = True      

