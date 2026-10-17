import json
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

from tools.inventory_tools import get_stock
from tools.po_tools import create_PO_draft


env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


TOOLS = [
    {
        "type": "function",
        "name": "get_stock",
        "description": "Get the current inventory level for a SKU at a specific store.",
        "parameters": {
            "type": "object",
            "properties": {
                "store_id": {
                    "type": "string",
                    "description": "Store ID such as S163"
                },
                "sku": {
                    "type": "string",
                    "description": "SKU such as SKU03782"
                }
            },
            "required": ["store_id", "sku"],
            "additionalProperties": False
        }
    },
    {
        "type": "function",
        "name": "create_PO_draft",
        "description": (
            "Create a draft purchase order. "
            "This creates only a DRAFT and never submits a real order."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "store_id": {
                    "type": "string",
                    "description": "Store ID such as S163"
                },
                "sku": {
                    "type": "string",
                    "description": "SKU such as SKU03782"
                },
                "quantity": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Number of units to include in the draft purchase order"
                },
                "supplier_id": {
                    "type": "string",
                    "description": (
                        "Optional supplier ID. "
                        "If the user does not provide a supplier ID, omit this field. "
                        "The backend will automatically use AUTO_SELECT_PENDING."
                    )
                }
            },
            "required": [
                "store_id",
                "sku",
                "quantity"
            ],
            "additionalProperties": False
        }
    }
]


INSTRUCTIONS = (
    "You are an omnichannel retail inventory assistant. "
    "Use get_stock when the user asks about current inventory. "
    "Use create_PO_draft only when the user explicitly asks to create "
    "a draft purchase order. "
    "For create_PO_draft, store_id, sku, and quantity are sufficient. "
    "If supplier_id is not provided by the user, do not ask for it. "
    "Call create_PO_draft without supplier_id so the backend uses "
    "AUTO_SELECT_PENDING. "
    "Never claim that a real purchase order was submitted. "
    "Maintain conversational context across follow-up questions. "
    "If the user refers to the previous store, SKU, quantity, or result, "
    "use the conversation context when appropriate."
)


def execute_tool(item):
    args = json.loads(item.arguments)

    if item.name == "get_stock":
        result = get_stock(
            store_id=args["store_id"],
            sku=args["sku"]
        )

    elif item.name == "create_PO_draft":
        result = create_PO_draft(
            store_id=args["store_id"],
            sku=args["sku"],
            quantity=args["quantity"],
            supplier_id=args.get("supplier_id") or "AUTO_SELECT_PENDING"
        )

    else:
        result = {
            "error": f"Unknown tool: {item.name}"
        }

    print(f"\n[Tool called: {item.name}]")
    print("Arguments:", args)
    print("Tool result:", result)

    return {
        "type": "function_call_output",
        "call_id": item.call_id,
        "output": json.dumps(result)
    }


def process_turn(user_question, previous_response_id=None):
    request_args = {
        "model": "gpt-5.6",
        "input": user_question,
        "tools": TOOLS,
        "instructions": INSTRUCTIONS
    }

    if previous_response_id:
        request_args["previous_response_id"] = previous_response_id

    response = client.responses.create(**request_args)

    while True:
        tool_outputs = []

        for item in response.output:
            if item.type == "function_call":
                tool_outputs.append(execute_tool(item))

        if not tool_outputs:
            break

        response = client.responses.create(
            model="gpt-5.6",
            previous_response_id=response.id,
            input=tool_outputs,
            tools=TOOLS,
            instructions=INSTRUCTIONS
        )

    return response


def main():
    print("=" * 60)
    print("OMNICHANNEL RETAIL INVENTORY ASSISTANT")
    print("=" * 60)
    print("Ask about stock or create a draft purchase order.")
    print("Type 'exit', 'quit', or 'bye' to close the assistant.")
    print("=" * 60)

    previous_response_id = None

    while True:
        try:
            user_question = input("\nYou: ").strip()

            if not user_question:
                continue

            if user_question.lower() in {"exit", "quit", "bye"}:
                print("\nAssistant: Session closed.")
                break

            response = process_turn(
                user_question,
                previous_response_id=previous_response_id
            )

            previous_response_id = response.id

            print("\nAssistant:")
            print(response.output_text)

        except KeyboardInterrupt:
            print("\n\nAssistant: Session closed.")
            break

        except Exception as e:
            print("\nAssistant error:")
            print(str(e))


if __name__ == "__main__":
    main()
