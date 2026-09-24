import html
import hmac
import os
import threading
from contextlib import contextmanager

import duckdb
import gradio as gr
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

# ── Config ──────────────────────────────────────────────────────────────────
PARQUET_URL = os.environ.get("LOOKUP_PARQUET_URL", "").strip()
API_KEY = os.environ.get("LOOKUP_API_KEY", "").strip()

if not PARQUET_URL:
    raise RuntimeError("LOOKUP_PARQUET_URL is not set")

# ── Connection pool (thread-local, no global race) ──────────────────────────
_local = threading.local()


def _new_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET home_directory='/tmp'")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute(
        "CREATE OR REPLACE VIEW users_view AS "
        f"SELECT * FROM read_parquet('{PARQUET_URL}')"
    )
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '1GB'")
    return con


def get_db() -> duckdb.DuckDBPyConnection:
    con = getattr(_local, "con", None)
    if con is None:
        con = _new_conn()
        _local.con = con
    return con


# ── Query ───────────────────────────────────────────────────────────────────
def search(mobile: str) -> dict | None:
    if not mobile or not mobile.strip():
        return None
    db = get_db()
    row = db.execute(
        "SELECT mobile, name, fname, address, circle, id "
        "FROM users_view WHERE mobile = ? LIMIT 1",
        [mobile.strip()],
    ).fetchone()
    if not row:
        return None
    keys = ["mobile", "name", "fname", "address", "circle", "id"]
    return dict(zip(keys, row))


# ── Auth ────────────────────────────────────────────────────────────────────
def _check_key(provided: str) -> bool:
    if not API_KEY:
        return False
    return hmac.compare_digest(provided or "", API_KEY)


# ── HTML rendering (escaped) ────────────────────────────────────────────────
def format_result(data: dict) -> str:
    if not data:
        return ""
    e = html.escape
    return f"""
    <div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:16px;
                padding:24px;font-family:'Segoe UI',sans-serif;color:#fff;">
        <div style="font-size:20px;font-weight:700;">{e(str(data.get('name') or ''))}</div>
        <div style="font-size:13px;color:#a0aec0;">{e(str(data.get('mobile') or ''))}</div>
        <div style="margin-top:12px;font-size:13px;">
            <div><strong>Father:</strong> {e(str(data.get('fname') or 'N/A'))}</div>
            <div><strong>Circle:</strong> {e(str(data.get('circle') or 'N/A'))}</div>
            <div><strong>Address:</strong> {e(str(data.get('address') or 'N/A'))}</div>
            <div><strong>ID:</strong> {e(str(data.get('id') or 'N/A'))}</div>
        </div>
    </div>
    """


# ── FastAPI ─────────────────────────────────────────────────────────────────
fastapi_app = FastAPI(title="Number Lookup")


@fastapi_app.get("/api/check")
async def api_check(
    request: Request,
    number: str = Query(..., min_length=4, max_length=20),
    apikey: str = Query(...),
):
    if not _check_key(apikey):
        raise HTTPException(status_code=401, detail="invalid api key")
    data = search(number)
    if not data:
        raise HTTPException(status_code=404, detail="not found")
    return JSONResponse(
        {
            "0": {
                "name": data.get("name") or "",
                "father name": data.get("fname") or "",
                "mobile": data.get("mobile") or "",
                "address": data.get("address") or "",
                "circle/sim": data.get("circle") or "",
                "id number": str(data.get("id") or ""),
            }
        }
    )


# ── Gradio ──────────────────────────────────────────────────────────────────
def handle_search(mobile: str):
    if not mobile or not mobile.strip():
        return gr.update(value="<p>Enter a mobile number</p>", visible=True), gr.update(visible=False)
    data = search(mobile)
    if not data:
        return gr.update(value=f"<p>No match for {html.escape(mobile)}</p>", visible=True), gr.update(visible=False)
    return gr.update(visible=False), gr.update(value=format_result(data), visible=True)


with gr.Blocks(title="Number Lookup", theme=gr.themes.Soft()) as demo:
    gr.Markdown("## Number Lookup")
    with gr.Row():
        mobile_input = gr.Textbox(placeholder="Enter mobile number...", container=False, scale=3)
        search_btn = gr.Button("Search", variant="primary", scale=1)
    status_text = gr.Markdown(visible=False)
    result_html = gr.HTML(visible=False)
    search_btn.click(handle_search, mobile_input, [status_text, result_html])
    mobile_input.submit(handle_search, mobile_input, [status_text, result_html])

app = gr.mount_gradio_app(fastapi_app, demo, path="/")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "7860")))
