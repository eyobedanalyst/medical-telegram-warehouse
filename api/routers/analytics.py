from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from api.database import SessionLocal
from api.schemas import TopProduct, VisualContentStats
from typing import List

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get(
    "/reports/top-products",
    response_model=List[TopProduct],
    summary="Top mentioned products/terms"
)
def top_products(
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    sql = text("""
        SELECT term, mentions
        FROM analytics_marts.fct_top_terms
        ORDER BY mentions DESC
        LIMIT :limit
    """)

    rows = db.execute(sql, {"limit": limit}).fetchall()
    return rows


@router.get(
    "/reports/visual-content",
    response_model=List[VisualContentStats],
    summary="Visual content usage by channel"
)
def visual_content_stats(db: Session = Depends(get_db)):
    sql = text("""
        SELECT
            channel_name,
            total_messages,
            images_count,
            images_count::float / NULLIF(total_messages, 0) AS image_ratio
        FROM analytics_marts.fct_visual_content_stats
        ORDER BY image_ratio DESC
    """)

    return db.execute(sql).fetchall()
