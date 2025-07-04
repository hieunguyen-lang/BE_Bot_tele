from typing import Optional, List
from .base import BaseSchema, TimestampSchema
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, validator, Field
import re
import uuid
class CommissionBySenderOut(BaseModel):
    nguoi_gui: str
    total_commission: float
    total_transactions: int
    total_amount: float
    total_fee: float
    hoa_hong_cuoi_cung: float