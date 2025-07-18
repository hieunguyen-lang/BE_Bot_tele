from typing import Optional, List
from .base import BaseSchema, TimestampSchema
from typing import Optional,Any, List
from datetime import datetime,date
from pydantic import BaseModel, validator, Field
import re
import uuid
class SearchPostOut(BaseModel):
    posturl: str
    name: str
    content: str
    content_created: str
    created_at: str
    delay: int
    reaction_count: int
    comment_count: int
    share_count: int
    image_url: str
    type: str


class CrawlerPostItem(BaseModel):
    # Post Info
    post_id: Optional[Any]
    parent_post_id: Optional[Any]
    post_type: Optional[Any]
    post_keyword: Optional[Any]
    post_url: Optional[Any]
    post_image: Optional[Any]
    post_created: Optional[Any]
    post_created_timestamp: Optional[Any]
    post_raw: Optional[Any]
    message: Optional[Any]
    type: Optional[Any]
    data_form_source: Optional[Any]

    # Post Interaction
    count_like: Optional[Any]
    count_share: Optional[Any]
    count_comments: Optional[Any]
    comments: Optional[Any]

    # Object, Brand, Service Info
    brand_id: Optional[Any]
    object_id: Optional[Any]
    service_id: Optional[Any]
    parent_object_id: Optional[Any]
    parent_service_id: Optional[Any]

    # Page Info
    page_id: Optional[Any]
    page_name: Optional[Any]

    # Author
    author_id: Optional[Any]
    author_name: Optional[Any]
    author_username: Optional[Any]
    author_image: Optional[Any]