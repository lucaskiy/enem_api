from fastapi import APIRouter
from core.db.queries import query_test, query_year, query_state
from core.db.conn import client

router = APIRouter(
    prefix="/notas",
    tags=["notas"],
    responses={404: {"description": "Not found"}},
)


@router.get("/enem_demo/")
async def read_users(limit: int = 10):
    query = query_test % limit
    res = client.query(query)
   
    result = res.result()
    final_result = list()

    for row in result:
        final_result.append(row)
    return final_result    


@router.get("/enem/{estado}")
async def read_users(estado: str, limit: int = 10):
    state = estado

    query = query_state % (state, limit)
    res = client.query(query)

    result = res.result()
    final_result = list()

    for row in result:
        final_result.append(row)
    return final_result    


@router.get("/enem/{ano}")
async def read_users(ano: int, limit: int = 10):
    year = ano

    query = query_year % (year, limit)
    res = client.query(query)

    result = res.result()
    final_result = list()

    for row in result:
        final_result.append(row)
    return final_result    