from fastapi import APIRouter, HTTPException, Depends
from ....services.visit_counter import VisitCounterService

router = APIRouter()

# Use a shared instance of VisitCounterService
counter_service = VisitCounterService()

@router.post("/visit/{page_id}")
def record_visit(page_id: str):
    """Record a visit for a website (sharded mode)"""
    try:
        counter_service.increment_visit(page_id)  # ✅ Store in batch buffer
        return {"status": "success", "message": f"Visit recorded for page {page_id} (sharded mode)"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}")
def get_visits(page_id: str):
    """Get visit count for a website"""
    try:
        count, served_via = counter_service.get_visit_count_with_source(page_id)
        return {"visits": count, "served_via": served_via}  # ✅ Return correct shard name
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
