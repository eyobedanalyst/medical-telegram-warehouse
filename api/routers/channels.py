from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from api.database import SessionLocal
from api.schemas import ChannelActivity
from typing import List

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get(
    "/channels/{channel_name}/activity",
    response_model=List[ChannelActivity],
    summary="Channel posting activity over time"
)
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    sql = text("""
        SELECT
            date::text AS date,
            message_count
        FROM analytics_marts.fct_channel_activity
        WHERE channel_name = :channel_name
        ORDER BY date
    """)

    rows = db.execute(sql, {"channel_name": channel_name}).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Channel not found")

    return rows
