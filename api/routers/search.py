from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from api.database import SessionLocal
from api.schemas import MessageSearchResult
from typing import List

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get(
    "/search/messages",
    response_model=List[MessageSearchResult],
    summary="Search messages by keyword"
)
def search_messages(
    query: str = Query(..., min_length=3),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    sql = text("""
        SELECT
            message_id,
            channel_name,
            message_text,
            created_at::text
        FROM analytics_marts.fct_messages
        WHERE message_text ILIKE :q
        ORDER BY created_at DESC
        LIMIT :limit
    """)

    return db.execute(
        sql, {"q": f"%{query}%", "limit": limit}
    ).fetchall()
