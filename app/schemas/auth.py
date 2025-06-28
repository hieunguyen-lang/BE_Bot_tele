from pydantic import BaseModel
from typing import Optional
from .base import BaseSchema

class Token(BaseSchema):
    access_token: str
    token_type: str

class TokenData(BaseSchema):
    username: Optional[str] = None
    role: Optional[str] = None 