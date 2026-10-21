import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GENAI_DIR = PROJECT_ROOT / "genai"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if str(GENAI_DIR) not in sys.path:
    sys.path.insert(0, str(GENAI_DIR))


# ---------------------------------------------------------
# Existing project modules
# ---------------------------------------------------------

from tools.inventory_tools import get_stock
from tools.po_tools import create_PO_draft
from agent import process_turn


# ---------------------------------------------------------
# FastAPI
# ---------------------------------------------------------

app = FastAPI(
    title="Omnichannel Retail Operations API",
    version="1.0.0",
)


# React runs on port 5173
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------
# Request models
# ---------------------------------------------------------

class ChatRequest(BaseModel):
    message: str
    previous_response_id: str | None = None


class DraftPORequest(BaseModel):
    store_id: str
    sku: str
    quantity: int
    supplier_id: str = ""


# ---------------------------------------------------------
# Health
# ---------------------------------------------------------

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "omnichannel-retail-api",
    }


# ---------------------------------------------------------
# Dashboard summary
# ---------------------------------------------------------

@app.get("/api/dashboard")
def dashboard():
    return {
        "stores": 201,
        "skus": 5000,
        "suppliers": 100,
        "inventory_records": 471000,
        "sales_rows": 5000000,
        "replenishment_recommendations": 26545,
        "draft_purchase_orders": 14398,
        "forecast_mape": 16.71,
        "streaming_latency_seconds": 29.499,
        "stockout_rate_pct": 0.0004,
    }


# ---------------------------------------------------------
# Live inventory lookup
# ---------------------------------------------------------

@app.get("/api/inventory/{store_id}/{sku}")
def inventory_lookup(store_id: str, sku: str):

    try:
        result = get_stock(
            store_id.strip().upper(),
            sku.strip().upper(),
        )

        return result

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Inventory lookup failed: {exc}",
        )


# ---------------------------------------------------------
# Draft PO
# ---------------------------------------------------------

@app.post("/api/draft-po")
def draft_po(request: DraftPORequest):

    if request.quantity <= 0:
        raise HTTPException(
            status_code=400,
            detail="Quantity must be greater than zero.",
        )

    try:
        result = create_PO_draft(
            store_id=request.store_id.strip().upper(),
            sku=request.sku.strip().upper(),
            quantity=request.quantity,
            supplier_id=request.supplier_id.strip(),
        )

        return result

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Draft PO creation failed: {exc}",
        )


# ---------------------------------------------------------
# AI Assistant
# ---------------------------------------------------------

@app.post("/api/chat")
def chat(request: ChatRequest):

    question = request.message.strip()

    if not question:
        raise HTTPException(
            status_code=400,
            detail="Message cannot be empty.",
        )

    try:
        response = process_turn(
            question,
            previous_response_id=request.previous_response_id,
        )

        return {
            "answer": response.output_text,
            "response_id": response.id,
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"AI Assistant failed: {exc}",
        )
