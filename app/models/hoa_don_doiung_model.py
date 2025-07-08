from sqlalchemy import Boolean, Column, String, Enum, Text,Integer,DateTime,BigInteger
import enum
from .base import BaseModel
from sqlalchemy.sql import func

class DoiUng(BaseModel):
    __tablename__ = "doi_ung"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    nha_cung_cap = Column(String(255), nullable=True)
    ten_khach_hang = Column(String(255), nullable=True)
    ma_khach_hang = Column(String(50), nullable=True)
    dia_chi = Column(String(500), nullable=True)
    ky_thanh_toan = Column(String(100), nullable=True)
    so_tien = Column(BigInteger, nullable=True)
    ma_giao_dich = Column(String(100), unique=True, nullable=True)
    thoi_gian = Column(DateTime, nullable=True)
    tai_khoan_the = Column(String(100), nullable=True)
    tong_phi = Column(BigInteger, nullable=True)
    trang_thai = Column(String(50), nullable=True)
    nguoi_gui  = Column(String(100), nullable=True)
    batch_id = Column(String(250), nullable=True)
    update_at = Column(DateTime, nullable=True)
    phi_phan_tram = Column(String(250), nullable=True)
    doi_tac = Column(String(250), nullable=True)
    key_redis = Column(String(250), nullable=True)
