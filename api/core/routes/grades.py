from fastapi import APIRouter
from core.db.queries import test1
from core.db.conn import client

router = APIRouter(
    prefix="/notas",
    tags=["notas"],
    responses={404: {"description": "Not found"}},
)



@router.get("/alunos/")
async def read_users():
    query = client.query(test1)
    print(query)
    result = query.result()
    for row in result:
        return row    