from typing import Any, Dict
import time
import json

# -------------------------
# 환경 변수
# -------------------------

import os
from dotenv import load_dotenv

load_dotenv()

def env_str(name: str, default:str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value

def env_int(name: str, default:int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default

PG_DSN              = env_str("PG_DSN")             # Postgres 연결 문자열

API_KEY             = env_str("API_KEY")            # 금융감독원 API 키
BASE_URL_DEPOSIT    = env_str("BASE_URL_DEPOSIT")   # 금융감독원 API - 예금 상품
BASE_URL_SAVING     = env_str("BASE_URL_SAVING")    # 금융감독원 API - 적금 상품

# PAGE_SIZE           = env_int("PAGE_SIZE", 100)      # 한번에 조회할 데이터 건수
START_PAGE          = env_int("START_PAGE", 1)       # 시작 페이지 번호
END_PAGE            = env_int("END_PAGE", 0)          # 0 이면 끝까지

HTTP_TIMEOUT        = env_int("HTTP_TIMEOUT", 10)    # HTTP 요청 타임아웃 (초)
RETRY_MAX           = env_int("RETRY_MAX", 5)        # HTTP 요청 재시도 횟수

LOG_LEVEL           = env_str("LOG_LEVEL", "INFO")   # 로그 레벨


# -------------------------
# 로깅
# -------------------------

import logging

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(name)s %(levelname)s :: %(message)s"
)
log = logging.getLogger("raw_ingest")


# -------------------------
# Create engine & Test connection
# -------------------------

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError

def get_engine() -> Engine:
    return create_engine(PG_DSN, future=True)

def test_connection(engine: Engine) -> None:
    try:
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        log.info("Database connection successful")
    except SQLAlchemyError as e:
        log.error("Database connection failed", exc_info=e)
        raise

engine = get_engine()
test_connection(engine)


# -------------------------
# 부트스트랩
# -------------------------
        
BOOTSTRAP_SQL = """
create extension if not exists pgcrypto;

create schema if not exists raw;

create table if not exists raw.finproduct_pages (
    ingest_id       uuid primary key default gen_random_uuid(),
    ingested_at     timestamptz not null default now(),
    now_page_no     int not null,
    max_page_no     int,
    base_url        text not null,
    query_params    jsonb not null,
    http_status     int not null,
    payload         jsonb not null
);

"""

def bootstrap(conn: Connection) -> None:
    conn.execute(text(BOOTSTRAP_SQL))
    conn.commit()
    log.info("Bootstrap completed")

with engine.begin() as conn:
    bootstrap(conn)

# -------------------------
# HTTP 호출
# -------------------------

from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
import httpx

class ApiError(Exception):
    pass

@retry(
    stop = stop_after_attempt(RETRY_MAX),
    wait=wait_exponential_jitter(initial=0.5, max=5),
    retry=retry_if_exception_type((httpx.HTTPError, ApiError)),
)

def fetch_page(
    client: httpx.Client,
    top_fin_grp_no: int,
    page_no: str) -> Dict[str, Any]:
    params = {
        "auth": API_KEY,
        "topFinGrpNo": top_fin_grp_no,
        "pageNo": page_no,
    }
    response = client.get(BASE_URL_DEPOSIT, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    try:
        js = response.json()
    except Exception as e:
        raise ApiError(f"Invalid JSON response (page={page_no})") from e
    
    result = js.get("result", js)
    items = result.get("youthPolicyList", result.get("items", []))
    if not isinstance(items, list):
        # 페이지가 비어있는 경우엔 list가 아닐 수 있음 → 그대로 통과시키되 로그만 남김
        log.warning("Unexpected items type on page %s: %s", page_no, type(items).__name__)

    return {
        "http_status": response.status_code,
        "json": js,
        "params": params,
    }

def extract_paging_meta(payload: Dict[str, Any]) -> tuple[int, int, int]:
    
    result = payload.get("result", payload)
    
    page_no = int(result.get("now_page_no", 0) or START_PAGE)
    max_page_no = int(result.get("max_page_no", 0))
    total_count = int(result.get("total_count", 0))

    return page_no, max_page_no, total_count

# -------------------------
# 메인 루프
# -------------------------
def ingest_one(engine: Engine, base_url: str, top_fin_grp_no: str, label: str | None = None) -> int:
    """
    base_url(예: DEPOSIT/SAVING) × top_fin_grp_no(예: 020000/030300) 조합 한 번을 수집.
    fetch_page가 BASE_URL_DEPOSIT 전역을 사용하므로, 여기서 잠시 바꿨다가 되돌린다.
    """
    global BASE_URL_DEPOSIT
    prev_url = BASE_URL_DEPOSIT
    BASE_URL_DEPOSIT = base_url
    try:
        log.info("Starting raw ingest [%s] → %s", label or f"{top_fin_grp_no}", base_url)
        inserted_rows = 0

        with httpx.Client() as client, engine.connect() as conn:
            page = max(1, START_PAGE)
            last_page_seen = 0

            while True:
                response = fetch_page(client, top_fin_grp_no, page)
                status = response["http_status"]
                payload = response["json"]
                params = response["params"]

                # 페이징 메타 파싱
                now_page_no, max_page_no, total_count = extract_paging_meta(payload)

                if last_page_seen == 0 and max_page_no:
                    last_page_seen = max_page_no
                    log.info("[%s] Paging detected: max_page_no=%s", label or top_fin_grp_no, last_page_seen)

                # RAW 데이터 저장 (기존 INSERT 그대로 유지)
                sql = text(
                    """
                    insert into raw.finproduct_pages
                    (now_page_no, max_page_no, base_url, query_params, http_status, payload)
                    values
                    (:now_page_no, :max_page_no, :base_url, :query_params, :http_status, :payload)
                    """
                )
                conn.execute(sql, {
                    "now_page_no": page,
                    "max_page_no": max_page_no,
                    "base_url": base_url,
                    "query_params": json.dumps(params),
                    "http_status": status,
                    "payload": json.dumps(payload),
                })
                conn.commit()
                inserted_rows += 1
                log.info("[%s] Inserted RAW page: now_page_no=%s status=%s", label or top_fin_grp_no, now_page_no or page, status)

                # 종료 조건
                if END_PAGE and page >= END_PAGE:
                    log.info("[%s] END_PAGE reached: %s", label or top_fin_grp_no, END_PAGE)
                    break

                if last_page_seen and page >= last_page_seen:
                    log.info("[%s] Reached last page: %s", label or top_fin_grp_no, last_page_seen)
                    break

                # 메타 없으면 items 비어있는지로 종료
                result = payload.get("result", payload)
                items = result.get("baseList", result.get("optionList", []))
                if isinstance(items, list) and len(items) == 0 and not max_page_no:
                    log.info("[%s] Empty items; stopping at page=%s", label or top_fin_grp_no, page)
                    break

                page += 1
                time.sleep(0.2)

        log.info("[OK] RAW ingest done [%s]. pages inserted=%s", label or top_fin_grp_no, inserted_rows)
        return inserted_rows
    finally:
        # 전역 URL 원복
        BASE_URL_DEPOSIT = prev_url

def main() -> None:
    engine = get_engine()

    # (product_kind, url, top_fin_grp_no)
    combos = [
        ("DEPOSIT", BASE_URL_DEPOSIT, "020000"),
        ("DEPOSIT", BASE_URL_DEPOSIT, "030300"),
        ("SAVING",  BASE_URL_SAVING,  "020000"),
        ("SAVING",  BASE_URL_SAVING,  "030300"),
    ]

    total = 0
    for kind, url, grp in combos:
        total += ingest_one(engine, url, grp, label=f"{kind}/{grp}")

    log.info("[OK] All combos done. total pages inserted=%s", total)   

if __name__ == "__main__":
    main()

