from fastapi import FastAPI

app = FastAPI()

@app.get("/hello")
async def read_root():
    return {"message": "Hello World"}