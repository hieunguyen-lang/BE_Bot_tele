from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.hoa_don_models import HoaDon
from app.database import async_session
from datetime import datetime, timedelta
from sqlalchemy import update, select
async def reset_khach_moi():
    while True:
        async with async_session() as session:
            now = datetime.utcnow()
            expired_time = now - timedelta(days=30)

            stmt = (
                update(HoaDon)
                .where(HoaDon.khach_moi == True)
                .where(HoaDon.created_at <= expired_time)
                .values(khach_moi=False)
            )
            result = await session.execute(stmt)
            await session.commit()
            print(f"[{now}] ✅ Reset {result.rowcount} 'khach_moi' → False")
        #86400
        await asyncio.sleep(86400)  # 24 giờ (1 ngày)




