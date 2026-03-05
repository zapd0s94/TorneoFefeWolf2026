from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.f1_api import router as f1_router

app = FastAPI(title="Torneo F1 API", version="1.0")

# CORS (para que Streamlit pueda llamar sin drama)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

app.include_router(f1_router, prefix="/f1", tags=["F1"])