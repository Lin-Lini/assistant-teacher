from fastapi import APIRouter
from pydantic import BaseModel
from .rephrase_client import rephrase


class RephraseRequest(BaseModel):
    text: str


class RephraseResponse(BaseModel):
    text: str


router = APIRouter(
    prefix="/rephrase",
    tags=["rephrase"],
)


@router.post("", response_model=RephraseResponse)
def rephrase_endpoint(req: RephraseRequest):
    return {"text": rephrase(req.text)}