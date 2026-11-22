from fastapi import FastAPI
from app.api.routes.health import router as health_router
from app.api.routes.sources import router as source_router
from app.api.routes.webhooks import router as webhook_router
from app.api.routes.events import router as events_router

app = FastAPI(
    title = "webhook-reconciliation-engine",
    description="A reliable webhook processing system",
    version= "1.0.0"
)
app.include_router(health_router)
app.include_router(source_router)
app.include_router(webhook_router)
app.include_router(events_router,prefix="/webhooks")