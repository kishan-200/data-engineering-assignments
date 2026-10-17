import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_DIR = PROJECT_ROOT / "genai" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DRAFT_FILE = OUTPUT_DIR / "po_drafts.jsonl"


def create_PO_draft(
    store_id: str,
    sku: str,
    quantity: int,
    supplier_id: str = "AUTO_SELECT_PENDING"
) -> dict:

    if quantity <= 0:
        return {
            "created": False,
            "message": "Quantity must be greater than zero."
        }

    po_id = f"GENAI-PO-{uuid.uuid4().hex[:8].upper()}"

    draft = {
        "po_id": po_id,
        "status": "DRAFT",
        "store_id": store_id,
        "sku": sku,
        "quantity": quantity,
        "supplier_id": supplier_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "GenAI Agent"
    }

    with open(DRAFT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(draft) + "\n")

    return {
        "created": True,
        **draft,
        "message": "Draft purchase order created successfully. No order has been submitted."
    }


if __name__ == "__main__":
    result = create_PO_draft(
        store_id="S163",
        sku="SKU03782",
        quantity=25
    )

    print(result)
