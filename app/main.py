from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import engine
from .models import Base
from .controllers import auth_controller, bill_data_controller, user_controller
import asyncio

app = FastAPI(title="FastAPI Project")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_controller.router, tags=["authentication"])
app.include_router(user_controller.router, prefix="/api/user", tags=["users"])
app.include_router(bill_data_controller.router, prefix="/api/hoa-don", tags=["hoadon"])
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.get("/")
async def read_root():
    return {"message": "Welcome to FastAPI Project"} 