from fastapi import APIRouter, Depends, HTTPException, status,Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, date
from typing import List, Literal
from typing import List
from ..database import get_db
from ..models import User, UserRole
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from ..auth import get_current_active_user, get_current_admin_user
from ..services import report_service
from typing import Any
router = APIRouter()

@router.get("/summary")
async def report_summary(
    type: Literal['day', 'week', 'month', 'year','hour'] = Query('day'),
    from_: date = Query(..., alias='from'),
    to: date = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    return await report_service.report_summary(type=type, from_=from_, to=to, db=db, current_user=current_user)
