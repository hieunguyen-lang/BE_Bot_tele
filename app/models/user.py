from sqlalchemy import Boolean, Column, String, Enum
import enum
from .base import BaseModel

class UserRole(str, enum.Enum):
    ADMIN = "admin"
    MANAGER = "manager"
    USER = "user"

class User(BaseModel):
    __tablename__ = "users"

    email = Column(String(255), unique=True, index=True)
    username = Column(String(255), unique=True, index=True)
    hashed_password = Column(String(255))
    role = Column(String(255))
    is_active = Column(Boolean, default=True) 