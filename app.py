"""
Financial Compliance Pipeline — Web Dashboard API
===================================================
Serves the dashboard UI and provides REST endpoints consumed by the frontend.

Endpoints:
  GET  /                        → static/index.html
  POST /api/submit              → add a single transaction to queue
  POST /api/upload              → upload a CSV of transactions
  GET  /api/status?user_id=     → list all transactions (optionally filtered)
  GET  /api/results?user_id=    → list completed results (optionally filtered)
"""

import csv
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, UploadFile, Form, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ─────────────────────────────────────────────────────────────────
CSV_PATH     = os.getenv("CSV_PATH", "transactions.csv")
RESULTS_PATH = os.getenv("RESULTS_PATH", "results.csv")
STATIC_DIR   = "static"

# Seed users shown in UI even before any transactions are submitted
KNOWN_USERS = ["Officer_Khoa", "Director_Khai", "Auditor_Vy"]

TX_FIELDNAMES = ["id", "user_id", "status", "description",
                 "room_id", "verdict", "submitted_at", "completed_at"]

app = FastAPI(title="Financial Compliance Pipeline Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── CSV helpers ─────────────────────────────────────────────────────────────
def _ensure_tx_csv():
    parent = os.path.dirname(CSV_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=TX_FIELDNAMES).writeheader()


def _load_txs() -> list[dict]:
    _ensure_tx_csv()
    with open(CSV_PATH, newline="") as f:
        return list(csv.DictReader(f))


def _save_txs(txs: list[dict]):
    tmp = CSV_PATH + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TX_FIELDNAMES)
        w.writeheader()
        for tx in txs:
            w.writerow({k: tx.get(k, "") for k in TX_FIELDNAMES})
    shutil.move(tmp, CSV_PATH)


def _next_id() -> int:
    txs = _load_txs()
    if not txs:
        return 1
    try:
        return max(int(t.get("id", 0)) for t in txs) + 1
    except ValueError:
        return len(txs) + 1


def _load_results(user_id: str = "") -> list[dict]:
    if not os.path.exists(RESULTS_PATH):
        return []
    with open(RESULTS_PATH, newline="") as f:
        rows = list(csv.DictReader(f))
    if user_id:
        rows = [r for r in rows if r.get("user_id", "") == user_id]
    return rows


# ── Routes ──────────────────────────────────────────────────────────────────
@app.get("/api/users")
async def get_users():
    """Return distinct user_ids from transactions.csv, merged with KNOWN_USERS."""
    txs = _load_txs()
    from_csv = [t.get("user_id", "").strip() for t in txs if t.get("user_id", "").strip()]
    merged = list(dict.fromkeys(KNOWN_USERS + from_csv))  # preserve order, dedup
    return JSONResponse(merged)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    index = Path(STATIC_DIR) / "index.html"
    if not index.exists():
        return HTMLResponse(
            "<h3>Error: static/index.html not found! "
            "Please place index.html in static/ folder</h3>",
            status_code=500,
        )
    return HTMLResponse(index.read_text(encoding="utf-8"))


class SubmitRequest(BaseModel):
    user_id: str
    description: str


@app.post("/api/submit")
async def submit_transaction(req: SubmitRequest):
    if not req.user_id.strip():
        raise HTTPException(400, "user_id is required")
    if not req.description.strip():
        raise HTTPException(400, "description is required")

    tx_id = _next_id()
    txs = _load_txs()
    txs.append({
        "id": tx_id,
        "user_id": req.user_id.strip(),
        "status": "pending",
        "description": req.description.strip(),
        "room_id": "",
        "verdict": "",
        "submitted_at": "",
        "completed_at": "",
    })
    _save_txs(txs)
    return {"status": "success", "tx_id": tx_id}


@app.post("/api/upload")
async def upload_csv(user_id: str = Form(...), file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Only .csv files are accepted")

    content = (await file.read()).decode("utf-8").splitlines()
    reader = csv.DictReader(content)

    if reader.fieldnames is None:
        raise HTTPException(400, "Empty or invalid CSV")

    fields_lower = [f.lower().strip() for f in reader.fieldnames]
    has_id   = "id" in fields_lower
    has_desc = "description" in fields_lower

    if not has_desc:
        raise HTTPException(400, "CSV must have a 'description' column")

    txs = _load_txs()
    count = 0
    next_id = _next_id()

    for row in reader:
        desc = row.get("description", "").strip()
        if not desc:
            continue
        txs.append({
            "id": next_id,
            "user_id": user_id.strip(),
            "status": "pending",
            "description": desc,
            "room_id": "",
            "verdict": "",
            "submitted_at": "",
            "completed_at": "",
        })
        next_id += 1
        count += 1

    if count == 0:
        raise HTTPException(400, "No valid rows found in CSV")

    _save_txs(txs)
    return {"status": "success", "count": count}


@app.get("/api/status")
async def get_user_status(user_id: str = ""):
    txs = _load_txs()
    if user_id:
        txs = [t for t in txs if t.get("user_id", "") == user_id]
    return JSONResponse(txs)


@app.get("/api/results")
async def get_user_results(user_id: str = ""):
    # Pull from results.csv — merge user_id from transactions.csv for filtering
    if not os.path.exists(RESULTS_PATH):
        return JSONResponse([])

    with open(RESULTS_PATH, newline="") as f:
        results = list(csv.DictReader(f))

    if user_id:
        # Enrich results with user_id from transactions.csv for filtering
        txs = _load_txs()
        tx_map = {str(t["id"]): t.get("user_id", "") for t in txs}
        results = [
            {**r, "user_id": tx_map.get(str(r.get("id", "")), "")}
            for r in results
        ]
        results = [r for r in results if r.get("user_id") == user_id]
    else:
        txs = _load_txs()
        tx_map = {str(t["id"]): t.get("user_id", "") for t in txs}
        results = [
            {**r, "user_id": tx_map.get(str(r.get("id", "")), "")}
            for r in results
        ]

    return JSONResponse(results)


# ── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _ensure_tx_csv()
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
