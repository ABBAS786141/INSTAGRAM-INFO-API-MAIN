import asyncio
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Tuple

import duckdb
import gradio as gr
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("icmr")

# ── Config ──────────────────────────────────────────────────────────────────
BASE = os.path.dirname(os.path.abspath(__file__))

# Single source file
HF_PARQUET_URL = os.environ.get(
    "ICMR_PARQUET_URL",
    "https://huggingface.co/datasets/hkumar91864/Inddatainonefile/resolve/main/users_data.parquet",
)
VIEW_NAME = "users_view"

IS_VERCEL = os.environ.get("VERCEL", "false").lower() == "true"
MAX_CONNS = int(os.environ.get("ICMR_MAX_CONNS", "4"))
THREADS_PER_CONN = int(os.environ.get("ICMR_THREADS_PER_CONN", "2"))
DUPLICATE_CAP = int(os.environ.get("ICMR_DUPLICATE_CAP", "2"))
ENABLE_CACHE = os.environ.get("ICMR_ENABLE_CACHE", "true").lower() == "true"
CACHE_SIZE = int(os.environ.get("ICMR_CACHE_SIZE", "500"))
CACHE_TTL = int(os.environ.get("ICMR_CACHE_TTL", "300"))
QUERY_TIMEOUT_S = float(os.environ.get("ICMR_QUERY_TIMEOUT", "15"))

# ✅ Fields returned to the user
SEARCH_FIELDS = ["mobile", "name", "fname", "address", "circle", "id", "email"]

# Columns we search by (map user intent → actual column)
# If you have a real aadhaar column, replace the value below.
SEARCHABLE_COLUMNS = {
    "mobile": "mobile",
    "aadhaar": "aadhaar",   # ← change if your column is named differently
}

# ── Cache ──────────────────────────────────────────────────────────────────
class SimpleCache:
    def __init__(self, max_size=500, ttl=300):
        self.cache = {}
        self.max_size = max_size
        self.ttl = ttl
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            item = self.cache.get(key)
            if not item:
                return None
            value, ts = item
            if time.time() - ts < self.ttl:
                return value
            del self.cache[key]
            return None

    def set(self, key, value):
        with self.lock:
            if len(self.cache) >= self.max_size:
                oldest = min(self.cache, key=lambda k: self.cache[k][1])
                del self.cache[oldest]
            self.cache[key] = (value, time.time())


query_cache = SimpleCache(max_size=CACHE_SIZE, ttl=CACHE_TTL)

# ── DuckDB Connection Pool ─────────────────────────────────────────────────
_conn_pool: "queue.Queue[duckdb.DuckDBPyConnection]" = None
_pool_lock = threading.Lock()
_pool_initialized = False


def _new_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    try:
        con.execute("SET home_directory='/tmp'")
        con.execute("SET extension_directory='/tmp/duckdb_extensions'")
    except Exception as e:
        log.warning("home/extension dir set failed: %s", e)

    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Single view over the remote parquet
    con.execute(
        f"CREATE OR REPLACE VIEW {VIEW_NAME} AS "
        f"SELECT * FROM read_parquet('{HF_PARQUET_URL}')"
    )

    con.execute(f"SET threads = {THREADS_PER_CONN}")
    con.execute("SET memory_limit = '512MB'")   # safe for Vercel
    con.execute("PRAGMA enable_object_cache")
    return con


def _init_pool():
    global _conn_pool, _pool_initialized
    if _pool_initialized:
        return
    with _pool_lock:
        if _pool_initialized:
            return
        import queue
        _conn_pool = queue.Queue(maxsize=MAX_CONNS)
        # Create lazily on first acquire, so startup stays fast
        _pool_initialized = True
        log.info("Connection pool ready (max=%d)", MAX_CONNS)


def _acquire_conn() -> duckdb.DuckDBPyConnection:
    _init_pool()
    try:
        return _conn_pool.get_nowait()
    except Exception:
        # Pool empty → create a new one (bounded by caller's discipline)
        return _new_conn()


def _release_conn(con: duckdb.DuckDBPyConnection) -> None:
    try:
        _conn_pool.put_nowait(con)
    except Exception:
        try:
            con.close()
        except Exception:
            pass


# ── Core Search ────────────────────────────────────────────────────────────
def _normalize(q: str) -> str:
    """Strip spaces, dashes, plus signs — keep digits and letters only."""
    return "".join(c for c in q if c.isalnum())


def _person_key(row: dict) -> tuple:
    return (
        (row.get("mobile") or "").strip(),
        (row.get("id") or "").strip(),
    )


def _cap_repeats(rows: list[dict]) -> list[dict]:
    """Cap repeats of the same person at DUPLICATE_CAP; keep only SEARCH_FIELDS."""
    seen: dict[tuple, int] = {}
    out = []
    for r in rows:
        k = _person_key(r)
        n = seen.get(k, 0)
        if n < DUPLICATE_CAP:
            seen[k] = n + 1
            out.append({k: v for k, v in r.items() if k in SEARCH_FIELDS})
    return out


def _run_search(column: str, value: str, limit: int) -> list[dict]:
    """Query a single column with a parameterized statement."""
    if column not in SEARCHABLE_COLUMNS:
        return []
    col = SEARCHABLE_COLUMNS[column]

    sql = (
        f"SELECT {', '.join(SEARCH_FIELDS)} "
        f"FROM {VIEW_NAME} "
        f"WHERE {col} = ? "
        f"LIMIT ?"
    )
    params = [value, limit * DUPLICATE_CAP + 20]

    con = _acquire_conn()
    try:
        cur = con.execute(sql, params)
        rows = cur.fetchall()
        if not rows:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.warning("query failed on %s=%s: %s", col, value, e)
        return []
    finally:
        _release_conn(con)


def _unified_search(q: str, limit: int = 10) -> dict:
    q = q.strip()
    if not q:
        return {"query": q, "count": 0, "results": []}

    normalized = _normalize(q)
    cache_key = f"{normalized}:{limit}"

    if ENABLE_CACHE:
        cached = query_cache.get(cache_key)
        if cached is not None:
            return cached

    is_num = normalized.isdigit() and len(normalized) >= 8

    if not is_num:
        result = {"query": q, "count": 0, "results": []}
    else:
        # Parallel search across both columns
        with ThreadPoolExecutor(max_workers=len(SEARCHABLE_COLUMNS)) as ex:
            futures = [
                ex.submit(_run_search, col, normalized, limit)
                for col in SEARCHABLE_COLUMNS
            ]
            all_rows: list[dict] = []
            for f in futures:
                try:
                    all_rows.extend(f.result(timeout=QUERY_TIMEOUT_S))
                except Exception as e:
                    log.warning("parallel task error: %s", e)

        results = _cap_repeats(all_rows)[:limit]
        result = {"query": q, "count": len(results), "results": results}

    if ENABLE_CACHE and result["results"]:
        query_cache.set(cache_key, result)
    return result


# ── FastAPI ────────────────────────────────────────────────────────────────
fastapi_app = FastAPI(title="ICMR Search API")


@fastapi_app.get("/")
def root():
    return {
        "app": "ICMR Search API",
        "version": "3.0.0",
        "platform": "Vercel" if IS_VERCEL else "Local",
        "source": HF_PARQUET_URL,
        "status": "operational",
        "fields": SEARCH_FIELDS,
        "searchable": list(SEARCHABLE_COLUMNS.keys()),
    }


@fastapi_app.get("/health")
def health():
    return {
        "status": "ok",
        "platform": "Vercel" if IS_VERCEL else "Local",
        "pool_size": _conn_pool.qsize() if _conn_pool else 0,
        "cache_size": len(query_cache.cache),
    }


@fastapi_app.get("/search")
async def search(
    q: str = Query(..., description="Search query (mobile / aadhaar)"),
    limit: int = Query(10, ge=1, le=100),
):
    q_val = q.strip()
    if not q_val:
        raise HTTPException(status_code=422, detail="Provide q parameter")

    cache_key = f"{_normalize(q_val)}:{limit}"
    if ENABLE_CACHE and query_cache.get(cache_key):
        cached = query_cache.get(cache_key)
        return JSONResponse(content={**cached, "success": bool(cached["count"])},
                            headers={"X-Cache": "HIT"})

    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, _unified_search, q_val, limit)

    return JSONResponse(
        content={
            "success": bool(data["count"]),
            "query": q_val,
            "count": data["count"],
            "results": data["results"],
        },
        headers={"X-Cache": "MISS"},
    )


# ── Gradio UI ──────────────────────────────────────────────────────────────
def format_result(row: dict) -> str:
    labels = {
        "name": ("👤", "Name"),
        "fname": ("👨", "Father"),
        "mobile": ("📱", "Mobile"),
        "id": ("🆔", "ID"),
        "email": ("📧", "Email"),
        "address": ("📍", "Address"),
        "circle": ("🌐", "Circle"),
    }
    lines = []
    for k, (icon, label) in labels.items():
        v = row.get(k)
        if v:
            lines.append(f"{icon} **{label}:** {v}")
    return "\n".join(lines)


def search_ui(query: str, limit: int) -> str:
    if not query or not query.strip():
        return "⚠️ Please enter a search query."

    q = query.strip()
    start = time.time()
    try:
        data = _unified_search(q, int(limit))
    except Exception as e:
        log.exception("ui search failed")
        return f"❌ **Error:** {e}"

    elapsed = (time.time() - start) * 1000
    if not data["results"]:
        return f"🔍 **Query:** `{q}`\n⚡ **Time:** {elapsed:.1f}ms\n\n❌ No results."

    header = (
        f"🔍 **Query:** `{q}`\n"
        f"📊 **Found:** {data['count']}\n"
        f"⚡ **Time:** {elapsed:.1f}ms\n\n---\n"
    )
    parts = [f"### Result {i}\n{format_result(r)}"
             for i, r in enumerate(data["results"], 1)]
    return header + "\n\n---\n\n".join(parts)


def build_ui():
    with gr.Blocks(title="ICMR Search", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# 🔍 ICMR Search")
        gr.Markdown("⚡ **Serverless** — single Parquet source")
        with gr.Row():
            query_input = gr.Textbox(label="Search Query",
                                     placeholder="Mobile or Aadhaar number…")
            limit_slider = gr.Slider(1, 50, value=10, step=1, label="Max Results")
        with gr.Row():
            search_btn = gr.Button("⚡ Search", variant="primary")
            clear_btn = gr.Button("🗑️ Clear", variant="secondary")
        output = gr.Markdown()

        search_btn.click(search_ui, [query_input, limit_slider], output)
        query_input.submit(search_ui, [query_input, limit_slider], output)
        clear_btn.click(lambda: ("", "🔍 Enter a query…"), None,
                        [query_input, output])
    return demo


demo = build_ui()
app = gr.mount_gradio_app(fastapi_app, demo, path="/")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "7860")))
