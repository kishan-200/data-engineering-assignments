import json
import requests
from pathlib import Path
from datetime import datetime, timezone

OUT = Path("data_lake/bronze/external/weather")
OUT.mkdir(parents=True, exist_ok=True)

# Representative location: Hyderabad
LAT = 17.3850
LON = 78.4867

URL = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": LAT,
    "longitude": LON,
    "current": [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "weather_code",
        "wind_speed_10m"
    ],
    "daily": [
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum"
    ],
    "forecast_days": 3,
    "timezone": "Asia/Kolkata"
}

response = requests.get(
    URL,
    params=params,
    timeout=30
)

response.raise_for_status()

data = response.json()

payload = {
    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    "source": "Open-Meteo",
    "location": {
        "name": "Hyderabad",
        "latitude": LAT,
        "longitude": LON
    },
    "weather": data
}

outfile = OUT / "hyderabad_latest_weather.json"

with open(outfile, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print("===== OPEN-METEO WEATHER INGESTION =====")
print("HTTP status:", response.status_code)
print("Saved:", outfile)

current = data["current"]

print("Temperature :", current["temperature_2m"])
print("Humidity    :", current["relative_humidity_2m"])
print("Precipitation:", current["precipitation"])
print("Wind speed  :", current["wind_speed_10m"])
