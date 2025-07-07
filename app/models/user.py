from sqlalchemy import Boolean, Column, String, Enum, Integer, ForeignKey
import enum
from .base import BaseModel

class UserRole(str, enum.Enum):
    ADMIN = "admin"
    MANAGER = "manager"
    USER = "user"

class User(BaseModel):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(50), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    role_id = Column(Integer, ForeignKey("roles.id"), nullable=True)
    role = Column(String(255))

    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, username={self.username})>" 