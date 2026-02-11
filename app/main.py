from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.gee_client import initialize_ee_from_env, run_gee_task

app = FastAPI()

@app.on_event("startup")
def startup_event():
    initialize_ee_from_env()

@app.get("/")
async def root():
    return {"message": "GEE Dynamic World API is running"}

@app.post("/chat")
async def chat(data: dict):
    try:
        result = run_gee_task(data)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
