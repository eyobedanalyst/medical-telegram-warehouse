from fastapi import FastAPI
from api.routers import analytics, channels, search, health

app = FastAPI(
    title="Medical Telegram Analytics API",
    version="1.0.0",
    description="Analytics API powered by dbt data marts"
)

app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(analytics.router, prefix="/api", tags=["Analytics"])
app.include_router(channels.router, prefix="/api", tags=["Channels"])
app.include_router(search.router, prefix="/api", tags=["Search"])


@app.get("/", summary="Root endpoint")
def root():
    return {"message": "API is running successfully"}
