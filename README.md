# City Spending Enricher (CSV → APIs → JSON)

A tiny Python CLI that reads a CSV of city expenses, enriches each row via public APIs (no auth keys), and writes a clean JSON (or CSV) file.

## Data Flow

```
expenses.csv  →  Geocoding + Weather + FX  →  enriched.json (default)
```

## Requirements

- Python 3.9+
- No frameworks
- Each outbound HTTP call uses a timeout ≤ 10s

APIs used:

- **Geocode**: Open‑Meteo Geocoding  
  `https://geocoding-api.open-meteo.com/v1/search?name={city}&country={country_code}&count=1`
- **Weather**: Open‑Meteo Forecast (current)  
  `https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true`
- **FX**: exchangerate.host convert  
  `https://api.exchangerate.host/convert?from={local_currency}&to=USD&amount={amount}`

## Quickstart

1) Create and activate a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

2) Put your input CSV at the repo root (defaults to `expenses.csv`). Example:
```csv
city,country_code,local_currency,amount
Bengaluru,IN,INR,1250.50
Berlin,DE,EUR,89.90
San Francisco,US,USD,42.00
Tokyo,JP,JPY,3600
```

3) Run the enricher
```bash
python enrich.py --input expenses.csv --output enriched.json --format json --pretty
```

Optional: write CSV (like the sample in the assignment)
```bash
python enrich.py -i expenses.csv -o enriched.csv --format csv
```

4) Inspect the output
```bash
cat enriched.json | head -n 50
```

## CLI Usage

```
usage: enrich.py [-h] [-i INPUT] [-o OUTPUT] [--format {json,csv}] [--pretty]
                 [--workers WORKERS] [--verbose]

Enrich expense rows (CSV → APIs → JSON/CSV).

options:
  -h, --help            show this help message and exit
  -i, --input           input CSV path (default: expenses.csv)
  -o, --output          output file path (default: enriched.json)
  --format              output format: json (default) or csv
  --pretty              pretty-print JSON
  --workers             number of parallel workers (default: 4 or CPU count)
  --verbose             print progress
```

## Output Shape (JSON)

Each item contains:
```json
{
  "city": "Berlin",
  "country_code": "DE",
  "local_currency": "EUR",
  "amount_local": 89.9,
  "fx_rate_to_usd": 1.07,
  "amount_usd": 96.19,
  "latitude": 52.52437,
  "longitude": 13.41053,
  "temperature_c": 12.3,
  "wind_speed_mps": 3.8,
  "retrieved_at": "2025-08-21T12:34:56Z"
}
```

If a particular API fails, the corresponding fields are `null` and the rest still populate.

## Testing

Run the lightweight unit tests (no network calls; they mock HTTP):
```bash
pytest -q
```

## Notes

- Respectful timeouts of 10 seconds per HTTP call are enforced.
- Concurrency speeds up multi-row files without breaking the per‑request timeout rule.
- No secrets/tokens are used or required.
