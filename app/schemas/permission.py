from pydantic import BaseModel
from typing import Optional

class PermissionBase(BaseModel):
    name: str
    description: Optional[str] = None

class PermissionCreate(PermissionBase):
    pass

class PermissionOut(PermissionBase):
    id: int
    class Config:
        orm_mode = True

class RoleBase(BaseModel):
    name: str
    description: Optional[str] = None

class RoleCreate(RoleBase):
    pass

class RoleOut(RoleBase):
    id: int
    class Config:
        orm_mode = True

class RolePermissionBase(BaseModel):
    role_id: int
    permission_id: int

class RolePermissionCreate(RolePermissionBase):
    pass

class RolePermissionOut(RolePermissionBase):
    id: int
    class Config:
        orm_mode = True

class UserPermissionBase(BaseModel):
    user_id: int
    permission_id: int

class UserPermissionCreate(UserPermissionBase):
    pass

class UserPermissionOut(UserPermissionBase):
    id: int
    class Config:
        orm_mode = True 