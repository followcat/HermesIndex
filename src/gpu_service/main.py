from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .core.inference import InferenceService
from .settings import settings

app = FastAPI(title="HermesIndex GPU Inference")
service = InferenceService()


class TextsPayload(BaseModel):
    texts: list[str]


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "model": settings.model_name,
        "dim": service.dim,
    }


@app.post("/embed")
def embed(payload: TextsPayload) -> dict:
    if not payload.texts:
        raise HTTPException(status_code=400, detail="No texts provided")
    embeddings = service.embed(payload.texts)
    return {
        "embeddings": embeddings.tolist(),
        "dim": service.dim,
        "model": settings.model_name,
    }


@app.post("/classify")
def classify(payload: TextsPayload) -> dict:
    if not payload.texts:
        raise HTTPException(status_code=400, detail="No texts provided")
    scores = service.classify(payload.texts)
    return {"nsfw_scores": scores}


@app.post("/infer")
def infer(payload: TextsPayload) -> dict:
    if not payload.texts:
        raise HTTPException(status_code=400, detail="No texts provided")
    embeddings, scores = service.infer(payload.texts)
    return {
        "embeddings": embeddings.tolist(),
        "nsfw_scores": scores,
        "dim": service.dim,
        "model": settings.model_name,
    }
