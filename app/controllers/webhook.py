from fastapi import APIRouter, Request
from pydantic import BaseModel
import hashlib
import logging

logger = logging.getLogger("webhook")
logger.setLevel(logging.INFO)

# file log nếu muốn
handler = logging.FileHandler("webhook.log")
handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(handler)

router = APIRouter()

# API KEY của bạn lấy từ hệ thống autobank
API_KEY = "81d9eb79903e5311a4871c92e45cdb37"

class WebhookPayload(BaseModel):
    TaskId: int
    RequestId: str = ""
    Message: str
    Success: bool
    Hash: str

def calc_hash(task_id: int, request_id: str) -> str:
    raw = f"{API_KEY}{task_id}{request_id}"
    return hashlib.md5(raw.encode()).hexdigest()

@router.post("/webhook")
async def webhook(payload: WebhookPayload):
    logger.info(f"Received webhook: {payload.dict()}")
    # Tính lại hash
    expected_hash = calc_hash(payload.TaskId, payload.RequestId)
    if payload.Hash != expected_hash:
        logger.warning(f"Invalid hash! Expected {expected_hash}, got {payload.Hash}")
        return {"status": "error", "reason": "Invalid hash"}

    # TODO: Xử lý kết quả (update DB, gửi thông báo, …)
    logger.info(f"✅ Webhook verified: TaskId={payload.TaskId}, Success={payload.Success}")
    return {"status": "ok"}
