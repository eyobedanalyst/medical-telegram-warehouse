from pydantic import BaseModel
from typing import List


class TopProduct(BaseModel):
    term: str
    mentions: int


class ChannelActivity(BaseModel):
    date: str
    message_count: int


class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    created_at: str


class VisualContentStats(BaseModel):
    channel_name: str
    total_messages: int
    images_count: int
    image_ratio: float
