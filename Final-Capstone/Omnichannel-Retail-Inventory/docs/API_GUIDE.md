\# FastAPI Backend Guide



\## Overview



The Omnichannel Retail Inventory Platform uses FastAPI to connect the React dashboard with backend inventory, analytics, and AI services.



\## 1. Health Check



\*\*Endpoint:\*\* `GET /api/health`



Purpose: Check backend service availability.



\## 2. Dashboard



\*\*Endpoint:\*\* `GET /api/dashboard`



Purpose: Retrieve dashboard KPI information.



\## 3. Inventory Lookup



\*\*Endpoint:\*\* `GET /api/inventory/{store\_id}/{sku}`



Example:



`GET /api/inventory/S163/SKU03782`



Purpose: Retrieve inventory information for a specific store and SKU.



Expected information includes:



\- Current stock

\- Reorder point

\- Maximum stock level

\- Stock status



\## 4. AI Assistant



\*\*Endpoint:\*\* `POST /api/chat`



Example JSON request:



```json

{

&#x20; "message": "What is the stock of SKU03782 at S163?"

}

```



Purpose: Submit inventory-related questions to the backend AI assistant.



\## 5. Draft Purchase Order



\*\*Endpoint:\*\* `POST /api/draft-po`



Purpose: Generate a draft purchase order for replenishment review.



A draft purchase order does not automatically place an order with a supplier.



\## 6. API Testing



When the backend is running locally, open:



`http://localhost:8000/docs`



FastAPI's interactive Swagger documentation can be used to inspect and test the available endpoints.



\## 7. Security



\- Keep API keys in backend environment variables.

\- Do not commit credentials to GitHub.

\- Validate incoming API requests.

\- Handle backend errors safely.



\## Notes



Endpoint names and request formats should be checked against the implemented FastAPI routes before the final demonstration.
