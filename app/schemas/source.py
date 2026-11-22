import uuid

from pydantic import BaseModel,Field,ConfigDict,SecretStr

class SourceCreate(BaseModel):
    
    name : str = Field(
        min_length=2,
        max_length=100
    )
    signing_secret : SecretStr = Field(
        min_length=16
    )
    
class SourceResponse(BaseModel):
    
    id : uuid.UUID
    name : str
    is_active : bool
    
    model_config = ConfigDict(from_attributes=True)
    
class SourceUpdate(BaseModel):
    
    name: str | None = Field(
        default=None,
        min_length=2,
        max_length=100
    )

    is_active: bool | None = None