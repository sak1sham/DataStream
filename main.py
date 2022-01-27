from fastapi import FastAPI
from routes.routes import api_router
import uvicorn

app = FastAPI()
app.include_router(api_router)

if __name__ == '__main__':
    uvicorn.run(app, port=8000, host='0.0.0.0')