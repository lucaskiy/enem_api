from fastapi import FastAPI
from core.routes import grades

app = FastAPI()
app.include_router(grades.router)


@app.get("/")
async def root():
    return {"message": "Hello World"}