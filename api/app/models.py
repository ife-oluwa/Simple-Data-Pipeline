from pydantic import BaseModel


class SearchUser(BaseModel):
    department: int
    company: int
    limit: int