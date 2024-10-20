from fastapi import FastAPI
from app.routes import upload, delete

app = FastAPI()

# Include the routes
app.include_router(upload.router, prefix="/api")
app.include_router(delete.router, prefix="/api")

@app.get("/")
def read_root():
    return {"message": "Welcome to the API"}