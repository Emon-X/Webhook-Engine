from sqlalchemy import select, func
from fastapi import APIRouter, Query, Response
from app.api.deps import DBsession
from app.schemas.source import SourceCreate, SourceResponse
from app.db.models import Source
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
import uuid
from app.core.security import encrypt_secret


router = APIRouter(tags=["sources"], prefix="/sources")

@router.post("/",response_model=SourceResponse)
def create_source(data : SourceCreate, db: DBsession):
    
    source = Source(name = data.name)
    
    encrypted_secret = encrypt_secret(data.signing_secret.get_secret_value())
    
    source = Source(
        name = data.name,
        signing_secret_encrypted = encrypted_secret
    ) 
    
    try:
        db.add(source)
        db.commit()
        db.refresh(source)
        
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code = 409,
            detail = f"Source with name {data.name} already exists",
        )
        
    return source

@router.get("/")
def get_sources(
    db: DBsession,
    page : int = Query(default = 1, ge = 1),
    limit : int = Query(default = 10, ge = 1,le = 100),
    is_active : bool | None = Query(default = None)
    
    ):
    
    filter = []
 
    if is_active is not None:
        filter.append(Source.is_active == is_active)
        
    count_statement = select(func.count()).select_from(Source)
    
    if filter:
        count_statement = count_statement.where(*filter)
    
    total = db.scalar(count_statement) or 0
    statement = select(Source)
    
    if filter:
        statement = statement.where(*filter)
    
    offset = (page - 1) * limit
    statement = (
        statement
        .order_by(Source.created_at.desc(),Source.id)
        .offset(offset)
        .limit(limit)
    )
    
    source = db.scalars(statement).all()
    
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "items": source,
        "pages": (total + limit - 1) // limit
    }
    
    
@router.get("/{source_id}",response_model=SourceResponse)
def get_source(source_id:uuid.UUID, db: DBsession):
    
    statement = select(Source).where(Source.id == source_id)
    source = db.scalar(statement)
    
    if not source:
        raise HTTPException(
            status_code = 404,
            detail = f"Source with id {source_id} not found",
        )
    
    return source

@router.patch("/{source_id}",response_model=SourceResponse)
def update_source(source_id:uuid.UUID, data:SourceCreate, db: DBsession):
    
    statement = select(Source).where(Source.id == source_id)
    source = db.scalar(statement)
    
    if not source:
        raise HTTPException(
            status_code = 404,
            detail = f"Source with id {source_id} not found",
        )
    
    update_data = data.model_dump(
        exclude_unset=True,
        exclude_none=True
    )
    
    if not update_data:
        raise HTTPException(
            status_code = 400,
            detail = "No data provided for update",
        )
        
    for field, value in update_data.items():
        setattr(source, field, value)
    
    try:
        db.commit()
        db.refresh(source)
        
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code = 409,
            detail = f"Source with name {data.name} already exists",
        )
    
    return source

@router.delete("/{source_id}",status_code=204)
def deactive_source(source_id:uuid.UUID, db: DBsession):
    
    statement = select(Source).where(Source.id == source_id)
    source = db.scalar(statement)
    
    if not source:
        raise HTTPException(
            status_code = 404,
            detail = f"Source with id {source_id} not found",
        )
    
    source.is_active = False
    db.commit()
    
    return Response(status_code=204, content=None)