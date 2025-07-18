import requests
import re
import random
import uuid
from lxml import html
from datetime import datetime,timedelta
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import cast, INT
from ..models.hoa_don_models import HoaDon
from ..models.hoa_don_momo_model import HoaDonDien
from ..models.hoa_don_doiung_model import DoiUng
from ..schemas.hoadon_schemas import HoaDonOut,HoaDonUpdate,HoaDonCreate
from .. schemas.hoadon_dien_schemas import HoaDonDienOut
from .. schemas.doiung_schemas import DoiUngOut
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
from ..helpers import helper, request_get_data
from ..schemas.search_schemas import SearchPostOut

async def searchSocial(filters):

    result = await request_get_data.get_request_data_instagram(filters.get("keyword"))
    

    return result

