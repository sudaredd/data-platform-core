from pydantic import BaseModel
from typing import Optional

class ChatRequest(BaseModel):
    query: str
    tenant_id: str

class ChatResponse(BaseModel):
    answer: str
    tool_used: Optional[str] = None
