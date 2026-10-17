import json
import requests
from pathlib import Path
from datetime import datetime, timezone

OUT = Path("data_lake/bronze/external/fx")
OUT.mkdir(parents=True, exist_ok=True)

URL = "https://api.frankfurter.dev/v2/rates"

params = {
    "base": "usd",
    "quotes": "inr,eur,gbp"
}

headers = {
    "User-Agent": "omnichannel-retail-capstone/1.0"
}

response = requests.get(
    URL,
    params=params,
    headers=headers,
    timeout=30
)

response.raise_for_status()

data = response.json()

payload = {
    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    "source": "Frankfurter",
    "base": "USD",
    "rates": data
}

outfile = OUT / "latest_usd_rates.json"

with open(outfile, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print("===== FRANKFURTER FX INGESTION =====")
print("HTTP status:", response.status_code)
print("Saved:", outfile)

for row in data:
    print(
        f"{row['base']} -> {row['quote']} "
        f"rate={row['rate']} date={row['date']}"
    )
