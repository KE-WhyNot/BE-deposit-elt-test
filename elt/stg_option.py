# stg_option.py
from __future__ import annotations

"""
finlife raw pages -> stg.finproduct_option_landing (DEPOSIT + SAVING 통합, Engine 버전)

- STG는 '페이지 원문(JSON)'을 '상품 옵션 단위'로 분해해 적재
- 얇은 STG: (product_type, ext_source, fin_prdt_cd, payload, content_hash)
- 생성 컬럼(조회/정렬 보조):
    dcls_month (TEXT)
    save_trm (INT)
    intr_rate_type, intr_rate_type_nm (TEXT)
    intr_rate, intr_rate2 (NUMERIC)
    rsrv_type, rsrv_type_nm (TEXT)  # saving 위주, deposit이면 NULL
- 중복 방지: (product_type, ext_source, fin_prdt_cd, content_hash) UNIQUE

환경변수:
  PG_DSN = postgresql+psycopg://user:pass@host:5432/db  (SQLAlchemy DSN 권장)

실행:
  python stg_option.py [all|deposit|saving]
"""

import os, sys, hashlib, logging
from typing import Any, Dict, Iterable, List

from dotenv import load_dotenv
import orjson

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB

# -----------------------------
# 설정
# -----------------------------
load_dotenv()

def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v in (None, ""):
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

PG_DSN = env_str("PG_DSN")

PRODUCTS = {
    "deposit": {
        "product_type": "DEPOSIT",
        "ext_source":   "finlife_deposit",
        "raw_table":    "raw.finproduct_pages",
    },
    "saving": {
        "product_type": "SAVING",
        "ext_source":   "finlife_saving",
        "raw_table":    "raw.finproduct_pages",
    },
}

FETCH_SIZE = 500      # 페이지 로우 스트리밍 단위(읽기)
BATCH_SIZE = 1000     # 옵션 INSERT 배치 단위(쓰기)

log = logging.getLogger("stg_finproduct_option_landing")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s :: %(message)s")

# -----------------------------
# 유틸
# -----------------------------
def norm_json(obj: Dict[str, Any]) -> bytes:
    # 키 정렬 → 해시 안정성
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def batched(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    buf: List[Dict[str, Any]] = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def extract_option_records(page_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    finlife 응답 예:
      { "result": { "baseList": [...], "optionList": [ {...}, ... ] }, ... }
    """
    result = page_payload.get("result") or {}
    option_list = result.get("optionList") or []
    out: List[Dict[str, Any]] = []
    for item in option_list:
        if not isinstance(item, dict):
            continue
        fin_prdt_cd = item.get("fin_prdt_cd")
        if not fin_prdt_cd:
            continue
        out.append({
            "fin_prdt_cd":  fin_prdt_cd,
            "payload":      item,                         # dict 그대로(JSONB로 바인딩)
            "content_hash": sha256_hex(norm_json(item)),
        })
    return out

# -----------------------------
# DDL (멱등)
# -----------------------------
DDL = """
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.finproduct_option_landing (
  run_ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
  product_type      TEXT        NOT NULL CHECK (product_type IN ('DEPOSIT','SAVING')),
  ext_source        TEXT        NOT NULL,  -- 'finlife_deposit' | 'finlife_saving'
  fin_prdt_cd       TEXT        NOT NULL,  -- 외부 상품코드
  payload           JSONB       NOT NULL,  -- 1 옵션 원본(JSON)
  content_hash      TEXT        NOT NULL,

  -- 생성 컬럼(조회/정렬/가독성)
  dcls_month        TEXT    GENERATED ALWAYS AS ((payload->>'dcls_month')) STORED,
  save_trm          INTEGER GENERATED ALWAYS AS (NULLIF(payload->>'save_trm','')::int) STORED,
  intr_rate_type    TEXT    GENERATED ALWAYS AS ((payload->>'intr_rate_type')) STORED,
  intr_rate_type_nm TEXT    GENERATED ALWAYS AS ((payload->>'intr_rate_type_nm')) STORED,
  intr_rate         NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'intr_rate','')::numeric) STORED,
  intr_rate2        NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'intr_rate2','')::numeric) STORED,
  rsrv_type         TEXT    GENERATED ALWAYS AS ((payload->>'rsrv_type')) STORED,
  rsrv_type_nm      TEXT    GENERATED ALWAYS AS ((payload->>'rsrv_type_nm')) STORED
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'stg' AND indexname = 'finproduct_option_landing_uq'
  ) THEN
    CREATE UNIQUE INDEX finproduct_option_landing_uq
      ON stg.finproduct_option_landing (product_type, ext_source, fin_prdt_cd, content_hash);
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS finproduct_option_landing_key
  ON stg.finproduct_option_landing (product_type, fin_prdt_cd, dcls_month, save_trm, intr_rate_type, rsrv_type);
"""

INSERT_SQL = text("""
INSERT INTO stg.finproduct_option_landing (
  product_type, ext_source, fin_prdt_cd, payload, content_hash
) VALUES (
  :product_type, :ext_source, :fin_prdt_cd, :payload, :content_hash
)
ON CONFLICT DO NOTHING
""").bindparams(bindparam("payload", type_=JSONB))   # dict → JSONB 매핑

# -----------------------------
# 실행
# -----------------------------
def bootstrap(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL))

def process_one_type(engine: Engine, key: str) -> tuple[int, int]:
    meta = PRODUCTS[key]
    product_type = meta["product_type"]
    ext_source   = meta["ext_source"]
    raw_table    = meta["raw_table"]

    pages = 0
    attempted_rows = 0

    # 읽기 전용 커넥션 (서버사이드 스트리밍)
    with engine.connect().execution_options(stream_results=True) as rconn:
        res = rconn.execute(text(
            f"SELECT payload FROM {raw_table} WHERE product_type = :pt"), {"pt": product_type})
        row_iter = res.mappings()  # {'payload': {...}}

        # 쓰기 커넥션 분리 (배치 커밋)
        with engine.connect() as wconn:
            records: List[Dict[str, Any]] = []
            for row in row_iter:
                pages += 1
                page_payload = row["payload"]
                recs = extract_option_records(page_payload)
                for r in recs:
                    r["product_type"] = product_type
                    r["ext_source"]   = ext_source
                records.extend(recs)

                # 옵션 BATCH_SIZE 기준으로 INSERT
                for batch in batched(records, BATCH_SIZE):
                    wconn.execute(INSERT_SQL, batch)
                    wconn.commit()
                    attempted_rows += len(batch)
                    records = []

            # 남은 레코드 flush
            if records:
                wconn.execute(INSERT_SQL, records)
                wconn.commit()
                attempted_rows += len(records)

    log.info("landed(option) | type=%s | pages=%d | rows=%d", key, pages, attempted_rows)
    return pages, attempted_rows

def run(which: str) -> None:
    if which not in ("all", "deposit", "saving"):
        raise SystemExit("Usage: python stg_option.py [all|deposit|saving]")

    engine = create_engine(PG_DSN, future=True)

    bootstrap(engine)

    total_pages = 0
    total_rows  = 0
    types = ("deposit", "saving") if which == "all" else (which,)

    for t in types:
        p, r = process_one_type(engine, t)
        total_pages += p
        total_rows  += r

    log.info("DONE(option) | types=%s | pages=%d | inserted_or_skipped=%d",
             ",".join(types), total_pages, total_rows)

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    run(arg)