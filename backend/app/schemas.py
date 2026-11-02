from pydantic import BaseModel, EmailStr, Field

class Signup(BaseModel):
    email: EmailStr
    password: str = Field(min_length=12, max_length=128)
    organization: str = Field(min_length=2, max_length=120)

class Login(BaseModel):
    email: EmailStr
    password: str

class QueryInput(BaseModel):
    sql: str = Field(min_length=1, max_length=10000)

class SaveQuery(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    sql: str = Field(min_length=1, max_length=10000)

class AIConfigInput(BaseModel):
    provider: str
    api_key: str = Field(min_length=10, max_length=400)
    model: str = Field(min_length=2, max_length=100)

class AskInput(BaseModel):
    prompt: str = Field(min_length=3, max_length=2000)
    provider: str


from typing import Literal
class EngineQueryInput(QueryInput):
    engine: Literal['auto','duckdb','spark'] = 'auto'
    use_cache: bool = True

class ScheduleInput(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    sql: str = Field(min_length=1, max_length=10000)
    engine: Literal['auto','duckdb','spark'] = 'auto'
    interval_minutes: int = Field(ge=5, le=10080)
