from typing import Optional, List
from .base import BaseSchema, TimestampSchema
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, validator, Field
import re
import uuid
class HoaDonBase(BaseSchema):
    thoi_gian: Optional[datetime] = None
    nguoi_gui: Optional[str] = None
    ten_khach: Optional[str] = None
    so_dien_thoai: Optional[str] = None
    type_dao_rut: Optional[str] = None
    ngan_hang: Optional[str] = None
    ngay_giao_dich: Optional[str] = None
    gio_giao_dich: Optional[str] = None
    tong_so_tien: Optional[str] = None
    so_the: Optional[str] = None
    tid: Optional[str] = None
    mid: Optional[str] = None
    so_lo: Optional[str] = None
    so_hoa_don: Optional[str] = None
    ten_may_pos: Optional[str] = None
    lich_canh_bao: Optional[str] = None
    tien_phi: Optional[str] = None
    batch_id: Optional[str] = None
    caption_goc: Optional[str] = None
    ket_toan: Optional[str] = None
    ck_vao: Optional[str] = None
    ck_ra: Optional[str] = None
    tinh_trang: Optional[str] = None
    ly_do: Optional[str] = None
    dia_chi: Optional[str] = None
    stk_khach: Optional[str] =  None
    stk_cty: Optional[str] =  None
    khach_moi: Optional[bool] =  None
    phan_tram_phi: Optional[str] =  None
    key_redis: Optional[str] = None

    
class HoaDonCreate(BaseModel):
    thoi_gian: datetime = datetime.now()
    ngay_giao_dich: Optional[str] = None
    nguoi_gui: str = Field(..., min_length=1, max_length=100)
    ten_khach: str = Field(..., min_length=1, max_length=100)
    so_dien_thoai: Optional[str] = None
    type_dao_rut: str = Field(..., description="Bắt buộc Đáo/Rút")
    tong_so_tien: str = Field(..., description="Bắt buộc nhập tổng số tiền")
    so_the: str = Field(..., description="Bắt buộc nhập số thẻ")
    tid: Optional[str] = Field(None, max_length=50)
    so_lo: Optional[str] = Field(None, max_length=50)
    so_hoa_don: Optional[str] = Field(None, max_length=50)
    gio_giao_dich: Optional[str] = None
    phan_tram_phi: str = Field(..., description="Bắt buộc nhập tiền phí %")
    tien_phi: str = Field(..., description="Bắt buộc nhập phí dịch vụ")
    ck_vao: Optional[str] = None
    ck_ra: Optional[str] = None
    stk_khach: Optional[str] =  None
    stk_cty: Optional[str] =  None
    dia_chi: Optional[str] = None
    mid: Optional[str] = Field(None, max_length=50)
    batch_id: str = str(uuid.uuid4())
    ly_do: Optional[str] = None
    caption_goc: Optional[str] = None
    key_redis: Optional[str] = None
    # ... các field khác
    @validator('type_dao_rut')
    def validate_type_dao_rut(cls, v):
        if v:
            if v.upper() not in {"DAO", "RUT"}:
                raise ValueError("type_dao_rut chỉ được phép là 'DAO' hoặc 'RUT'")
            return v.upper()
        return v
    @validator('ngay_giao_dich')
    def validate_ngay_giao_dich(cls, v):
        if v:
            try:
                datetime.strptime(v, '%Y-%m-%d')
            except ValueError:
                raise ValueError('Ngày giao dịch không đúng định dạng YYYY-MM-DD')
        return v
    
    @validator('gio_giao_dich')
    def validate_gio_giao_dich(cls, v):
        if v and not re.match(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$', v):
            raise ValueError('Giờ giao dịch không đúng định dạng HH:MM')
        return v
    
    @validator('tong_so_tien', 'tien_phi', 'ck_vao', 'ck_ra')
    def validate_number_fields(cls, v):
        if v:
            try:
                num = int(v)
                if num < 0:
                    raise ValueError('Giá trị không được âm')
            except ValueError:
                raise ValueError('Phải là số nguyên')
        return v
    
    @validator('so_dien_thoai', check_fields=False)
    def validate_phone(cls, v):
        if v and not re.match(r'^[0-9]{10,11}$', v):
            raise ValueError('Số điện thoại không hợp lệ')
        return v

class HoaDonUpdate(HoaDonBase):
    pass

class HoaDonOut(HoaDonBase):
    id: int
    class Config:
        orm_mode = True      

