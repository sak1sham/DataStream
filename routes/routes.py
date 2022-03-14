from fastapi import APIRouter

router = APIRouter()

@router.get("/api/dms/test/")
async def root():
    return {"message": "testback"}