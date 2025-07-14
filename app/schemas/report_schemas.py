from typing import Optional, List
from .base import BaseSchema, TimestampSchema
from typing import Optional
from datetime import datetime,date
from pydantic import BaseModel, validator, Field
import re
import uuid
class CommissionBySenderOut(BaseModel):
    nguoi_gui: str
    total_commission: float
    total_transactions: int
    total_amount: float
    total_fee: float
    total_commission_momo: float
    total_transactions_momo: int
    total_amount_momo: float
    total_fee_momo: float
    hoa_hong_cuoi_cung: float

# Mô hình dữ liệu trả về cho calendar
class HoaDonCalendarEvent(BaseModel):
    id: str
    title: str
    start: date
    ten_khach: Optional[str]
    nguoi_gui: Optional[str]
    so_dien_thoai: Optional[str]
    batch_id: Optional[str]
    thoi_gian: Optional[datetime]
