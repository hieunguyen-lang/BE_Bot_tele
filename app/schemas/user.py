from pydantic import EmailStr, validator,Field
from typing import Optional,List
from .base import BaseSchema, TimestampSchema
import re
class UserBase(BaseSchema):
    email: EmailStr = Field(..., description="Email người dùng")
    username: str = Field(..., min_length=3, max_length=30)
    permissions: List[str] = []
    role_id: Optional[int]
    role: Optional[str]
    @validator("username")
    def validate_username(cls, v):
        if not re.match(r"^[a-zA-Z0-9_]{3,30}$", v):
            raise ValueError("Username chỉ chứa chữ, số và dấu _ (3-30 ký tự)")
        return v

    # @validator("name")
    # def validate_name(cls, v):
    #     if len(v.strip()) < 2:
    #         raise ValueError("Tên phải có ít nhất 2 ký tự")
    #     if not re.match(r"^[a-zA-ZÀ-ỹ\s'-]+$", v):
    #         raise ValueError("Tên chỉ được chứa chữ cái và khoảng trắng")
    #     return v

    @validator("email")
    def validate_email(cls, v):
        # EmailStr đã kiểm tra định dạng, nhưng có thể thêm logic nếu cần
        allowed_domains = ["gmail.com", "yahoo.com", "example.com"]
        domain = v.split("@")[-1]
        if domain not in allowed_domains:
            raise ValueError("Chỉ chấp nhận email thuộc các domain: " + ", ".join(allowed_domains))
        return v
class UserCreate(UserBase):
    password: str = Field(..., min_length=8, max_length=128)

    @validator("password")
    def validate_password(cls, v):
        if not re.search(r"[A-Z]", v):
            raise ValueError("Mật khẩu phải có ít nhất 1 chữ hoa")
        if not re.search(r"[a-z]", v):
            raise ValueError("Mật khẩu phải có ít nhất 1 chữ thường")
        if not re.search(r"[0-9]", v):
            raise ValueError("Mật khẩu phải có ít nhất 1 số")
        if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", v):
            raise ValueError("Mật khẩu phải có ít nhất 1 ký tự đặc biệt")
        return v

class UserUpdate(BaseSchema):
    email: Optional[EmailStr] = None
    username: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None

class User(UserBase, TimestampSchema):
    id: int
    is_active: bool 

    class Config:
        orm_mode = True


