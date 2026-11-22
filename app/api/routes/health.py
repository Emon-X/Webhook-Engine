from fastapi import APIRouter,HTTPException
from app.db.session import engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError



router = APIRouter()


@router.get("/health")
def health_check():
    
    return {
        "status":"healthy",
        "application": "webhook-reconciliation-engine"
    }
    
@router.get("/health/db")
def db_health_check():
    
    try:
        with engine.connect() as connection:
        
            connection.execute(text("select 1"))
        return {
            "status":"healthy",
            "database":"connected"
        }
            
    except SQLAlchemyError as exc:
        
        raise HTTPException(
            status_code = 503,
            detail="database connection failed"
        )from exc