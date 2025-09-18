# stg_to_core.py
from __future__ import annotations
"""
STG(통합) -> CORE 업서트 + 옵션 세트 해시 계산

전제:
  - STG(base):   stg.finproduct_base_landing
  - STG(option): stg.finproduct_option_landing
  - product_type: 'DEPOSIT' | 'SAVING'
  - is_current로 현재본 1건 보장 (SCD2 스타일)

환경:
  PG_DSN = postgresql+psycopg://user:pass@host:5432/db
실행:
  python stg_to_core.py [all|deposit|saving]
"""

import os, sys, logging
from typing import Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()
log = logging.getLogger("stg_to_core")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s :: %(message)s")

def env_str(name: str, default: str | None=None) -> str:
    v = os.getenv(name, default)
    if v in (None, ""):
        raise RuntimeError(f"Missing env: {name}")
    return v

PG_DSN = env_str("PG_DSN")

PRODUCT_TYPES = {
    "deposit": "DEPOSIT",
    "saving":  "SAVING",
}

# ------------------------
# BOOTSTRAP (멱등)
# ------------------------
BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS core;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- product
CREATE TABLE IF NOT EXISTS core.product (
  id              BIGSERIAL PRIMARY KEY,
  product_type    TEXT NOT NULL CHECK (product_type IN ('DEPOSIT','SAVING')),
  ext_source      TEXT NOT NULL,
  ext_id          TEXT NOT NULL,            -- fin_prdt_cd
  payload         JSONB NOT NULL,
  content_hash    TEXT  NOT NULL,

  -- 조회 보조 생성 컬럼
  dcls_month      TEXT GENERATED ALWAYS AS ((payload->>'dcls_month')) STORED,
  fin_prdt_nm     TEXT GENERATED ALWAYS AS ((payload->>'fin_prdt_nm')) STORED,
  fin_co_no       TEXT GENERATED ALWAYS AS ((payload->>'fin_co_no')) STORED,
  kor_co_nm       TEXT GENERATED ALWAYS AS ((payload->>'kor_co_nm')) STORED,

  -- SCD2
  is_current      BOOLEAN    NOT NULL DEFAULT TRUE,
  valid_from_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to_ts     TIMESTAMPTZ NULL,

  -- 옵션 세트 서명
  options_set_hash TEXT NULL,
  options_count    INT  NULL,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 동일 외부키 현재본 1건 보장
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_current
  ON core.product(ext_source, ext_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_product_lookup
  ON core.product(ext_source, ext_id, is_current);
CREATE INDEX IF NOT EXISTS idx_product_dcls_month
  ON core.product(dcls_month) WHERE is_current;

-- option
CREATE TABLE IF NOT EXISTS core.product_option (
  id                BIGSERIAL PRIMARY KEY,
  product_id        BIGINT NOT NULL REFERENCES core.product(id) ON DELETE CASCADE,
  payload           JSONB  NOT NULL,
  content_hash      TEXT   NOT NULL,

  -- 조회/정렬 생성 컬럼
  dcls_month        TEXT    GENERATED ALWAYS AS ((payload->>'dcls_month')) STORED,
  save_trm          INTEGER GENERATED ALWAYS AS (NULLIF(payload->>'save_trm','')::int) STORED,
  intr_rate_type    TEXT    GENERATED ALWAYS AS ((payload->>'intr_rate_type')) STORED,
  intr_rate_type_nm TEXT    GENERATED ALWAYS AS ((payload->>'intr_rate_type_nm')) STORED,
  intr_rate         NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'intr_rate','')::numeric) STORED,
  intr_rate2        NUMERIC GENERATED ALWAYS AS (NULLIF(payload->>'intr_rate2','')::numeric) STORED,
  rsrv_type         TEXT    GENERATED ALWAYS AS ((payload->>'rsrv_type')) STORED,
  rsrv_type_nm      TEXT    GENERATED ALWAYS AS ((payload->>'rsrv_type_nm')) STORED,

  -- SCD2
  is_current        BOOLEAN    NOT NULL DEFAULT TRUE,
  valid_from_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to_ts       TIMESTAMPTZ NULL,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 동일 옵션키 현재본 1건 보장
CREATE UNIQUE INDEX IF NOT EXISTS uq_option_current
  ON core.product_option(product_id,
                          COALESCE(save_trm,-1),
                          COALESCE(intr_rate_type,''),
                          COALESCE(rsrv_type,''))
  WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_option_product
  ON core.product_option(product_id, is_current);
"""

# ------------------------
# BASE 업서트
# ------------------------
BASE_CLOSE_SQL = """
WITH candidates AS (
  SELECT
    b.product_type, b.ext_source, b.fin_prdt_cd AS ext_id, b.payload, b.content_hash, b.run_ts, b.dcls_month,
    ROW_NUMBER() OVER (
      PARTITION BY b.product_type, b.ext_source, b.fin_prdt_cd
      ORDER BY to_date(COALESCE(b.dcls_month,'190001'),'YYYYMM') DESC, b.run_ts DESC
    ) AS rn
  FROM stg.finproduct_base_landing b
  WHERE b.product_type = :product_type
),
to_close AS (
  SELECT p.id
  FROM core.product p
  JOIN candidates c
    ON c.rn = 1
   AND p.is_current = TRUE
   AND p.product_type = c.product_type
   AND p.ext_source  = c.ext_source
   AND p.ext_id      = c.ext_id
   AND p.content_hash <> c.content_hash
)
UPDATE core.product p
SET is_current = FALSE, valid_to_ts = now(), updated_at = now()
FROM to_close x
WHERE p.id = x.id
RETURNING p.id;
"""

BASE_INSERT_SQL = """
WITH candidates AS (
  SELECT
    b.product_type, b.ext_source, b.fin_prdt_cd AS ext_id, b.payload, b.content_hash, b.run_ts, b.dcls_month,
    ROW_NUMBER() OVER (
      PARTITION BY b.product_type, b.ext_source, b.fin_prdt_cd
      ORDER BY to_date(COALESCE(b.dcls_month,'190001'),'YYYYMM') DESC, b.run_ts DESC
    ) AS rn
  FROM stg.finproduct_base_landing b
  WHERE b.product_type = :product_type
),
need_insert AS (
  SELECT c.*
  FROM candidates c
  LEFT JOIN core.product p
    ON p.is_current = TRUE
   AND p.product_type = c.product_type
   AND p.ext_source  = c.ext_source
   AND p.ext_id      = c.ext_id
  WHERE c.rn = 1 AND (p.id IS NULL OR p.content_hash <> c.content_hash)
)
INSERT INTO core.product (
  product_type, ext_source, ext_id, payload, content_hash,
  is_current, valid_from_ts, created_at, updated_at
)
SELECT
  product_type, ext_source, ext_id, payload, content_hash,
  TRUE, now(), now(), now()
FROM need_insert
RETURNING id;
"""

BASE_TOUCH_SQL = """
WITH candidates AS (
  SELECT
    b.product_type, b.ext_source, b.fin_prdt_cd AS ext_id, b.payload, b.content_hash, b.run_ts, b.dcls_month,
    ROW_NUMBER() OVER (
      PARTITION BY b.product_type, b.ext_source, b.fin_prdt_cd
      ORDER BY to_date(COALESCE(b.dcls_month,'190001'),'YYYYMM') DESC, b.run_ts DESC
    ) AS rn
  FROM stg.finproduct_base_landing b
  WHERE b.product_type = :product_type
)
UPDATE core.product p
SET updated_at = now()
FROM candidates c
WHERE c.rn = 1
  AND p.is_current = TRUE
  AND p.product_type = c.product_type
  AND p.ext_source  = c.ext_source
  AND p.ext_id      = c.ext_id
  AND p.content_hash = c.content_hash
RETURNING p.id;
"""

# ------------------------
# OPTION 업서트 (현재본 base와만 매칭)
# ------------------------
OPTION_UPSERT_SQL = """
-- 현재본 product들과 매칭되는 옵션 "원천" 후보
WITH base_now AS (
  SELECT p.id AS product_id, p.ext_source, p.ext_id, p.dcls_month
  FROM core.product p
  WHERE p.is_current = TRUE
    AND p.product_type = :product_type
),

-- 0) 원천 후보 + 옵션키/유효월 파생
opt_raw AS (
  SELECT
    o.*,
    bn.product_id,
    bn.dcls_month AS base_month,
    /* 옵션키(정규화) */
    COALESCE(NULLIF(o.payload->>'save_trm','')::int, -1) AS k_save_trm,
    COALESCE(o.payload->>'intr_rate_type','')           AS k_intr_rate_type,
    COALESCE(o.payload->>'rsrv_type','')                AS k_rsrv_type,
    /* 유효 비교용 공시월: 옵션에 없으면 base월 사용 */
    COALESCE(o.dcls_month, bn.dcls_month)               AS eff_month
  FROM stg.finproduct_option_landing o
  JOIN base_now bn
    ON bn.ext_source = o.ext_source
   AND bn.ext_id     = o.fin_prdt_cd
   AND (o.dcls_month IS NULL OR o.dcls_month = bn.dcls_month)
  WHERE o.product_type = :product_type
),

-- 1) 동일 (product_id, 옵션키) 내에서 '최신 1건'만 남김
--    기준: eff_month DESC → run_ts DESC → content_hash DESC
opt_canon AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id, k_save_trm, k_intr_rate_type, k_rsrv_type
      ORDER BY to_date(COALESCE(eff_month,'190001'), 'YYYYMM') DESC,
               run_ts DESC,
               content_hash DESC
    ) AS rn
  FROM opt_raw
),

opt_candidates AS (
  SELECT * FROM opt_canon WHERE rn = 1
),

-- 2) 변경된 현재본 옵션 해제
closed AS (
  UPDATE core.product_option po
  SET is_current = FALSE, valid_to_ts = now(), updated_at = now()
  FROM opt_candidates oc
  WHERE po.is_current = TRUE
    AND po.product_id = oc.product_id
    AND COALESCE(po.save_trm,-1)       = oc.k_save_trm
    AND COALESCE(po.intr_rate_type,'') = oc.k_intr_rate_type
    AND COALESCE(po.rsrv_type,'')      = oc.k_rsrv_type
    AND po.content_hash <> oc.content_hash
  RETURNING po.product_id
),

-- 3) 신규 현재본 옵션 INSERT (없거나 바뀐 경우)
inserted AS (
  INSERT INTO core.product_option (
    product_id, payload, content_hash,
    is_current, valid_from_ts, created_at, updated_at
  )
  SELECT
    oc.product_id, oc.payload, oc.content_hash,
    TRUE, now(), now(), now()
  FROM opt_candidates oc
  LEFT JOIN core.product_option cur
    ON cur.is_current = TRUE
   AND cur.product_id = oc.product_id
   AND COALESCE(cur.save_trm,-1)       = oc.k_save_trm
   AND COALESCE(cur.intr_rate_type,'') = oc.k_intr_rate_type
   AND COALESCE(cur.rsrv_type,'')      = oc.k_rsrv_type
  WHERE cur.id IS NULL OR cur.content_hash <> oc.content_hash
  RETURNING product_id
),

-- 4) 동일 내용 옵션 touch
touched AS (
  UPDATE core.product_option po
  SET updated_at = now()
  FROM opt_candidates oc
  WHERE po.is_current = TRUE
    AND po.product_id = oc.product_id
    AND COALESCE(po.save_trm,-1)       = oc.k_save_trm
    AND COALESCE(po.intr_rate_type,'') = oc.k_intr_rate_type
    AND COALESCE(po.rsrv_type,'')      = oc.k_rsrv_type
    AND po.content_hash = oc.content_hash
  RETURNING po.product_id
)

SELECT
  (SELECT count(*) FROM closed)   AS closed_cnt,
  (SELECT count(*) FROM inserted) AS inserted_cnt,
  (SELECT count(*) FROM touched)  AS touched_cnt;
"""

# ------------------------
# 옵션 세트 해시/개수 갱신
# ------------------------
RECOMPUTE_OPTION_SET_HASH_SQL = """
WITH base_now AS (
  SELECT p.id AS product_id
  FROM core.product p
  WHERE p.is_current = TRUE
    AND p.product_type = :product_type
),
affected AS (  -- 이번 배치에서 고려된 상품만(최적화 목적)
  SELECT DISTINCT bn.product_id
  FROM base_now bn
),
agg AS (
  SELECT
    po.product_id,
    COUNT(*) AS options_count,
    encode(
      digest(
        string_agg(
          format(
            '%s|%s|%s|%s|%s',
            COALESCE(po.save_trm, -1),
            COALESCE(po.intr_rate_type, ''),
            COALESCE(po.rsrv_type, ''),
            COALESCE(TO_CHAR(po.intr_rate,  'FM999999999.00000'), ''),
            COALESCE(TO_CHAR(po.intr_rate2, 'FM999999999.00000'), '')
          ),
          ';'  -- 구분자
          ORDER BY po.save_trm, po.intr_rate_type, po.rsrv_type, po.intr_rate, po.intr_rate2
        ),
        'sha256'
      ),
      'hex'
    ) AS options_set_hash
  FROM core.product_option po
  WHERE po.is_current = TRUE
    AND po.product_id IN (SELECT product_id FROM affected)
  GROUP BY po.product_id
)
UPDATE core.product p
SET options_set_hash = a.options_set_hash,
    options_count    = a.options_count,
    updated_at       = now()
FROM agg a
WHERE p.id = a.product_id
  AND p.is_current = TRUE
  AND p.product_type = :product_type;

-- 옵션이 0개인 경우 처리
WITH base_now AS (
  SELECT p.id AS product_id
  FROM core.product p
  WHERE p.is_current = TRUE
    AND p.product_type = :product_type
),
zero AS (
  SELECT bn.product_id
  FROM base_now bn
  WHERE NOT EXISTS (
    SELECT 1 FROM core.product_option po
    WHERE po.product_id = bn.product_id AND po.is_current = TRUE
  )
)
UPDATE core.product p
SET options_set_hash = NULL,
    options_count    = 0,
    updated_at       = now()
FROM zero z
WHERE p.id = z.product_id
  AND p.is_current = TRUE
  AND p.product_type = :product_type;
"""

def upsert_for_type(conn, product_type: str) -> Tuple[int, int, int]:
    # 1) BASE: close / insert / touch
    conn.execute(text(BASE_CLOSE_SQL), {"product_type": product_type})
    conn.execute(text(BASE_INSERT_SQL), {"product_type": product_type})
    conn.execute(text(BASE_TOUCH_SQL), {"product_type": product_type})

    # 2) OPTION: upsert (close/insert/touch 카운트 로그용)
    res = conn.execute(text(OPTION_UPSERT_SQL), {"product_type": product_type}).mappings().first()
    closed_cnt   = int(res["closed_cnt"])
    inserted_cnt = int(res["inserted_cnt"])
    touched_cnt  = int(res["touched_cnt"])

    # 3) 옵션 세트 해시/개수 재계산
    conn.execute(text(RECOMPUTE_OPTION_SET_HASH_SQL), {"product_type": product_type})

    return closed_cnt, inserted_cnt, touched_cnt

def run(which: str) -> None:
    if which not in ("all", "deposit", "saving"):
        raise SystemExit("Usage: python stg_to_core.py [all|deposit|saving]")

    engine: Engine = create_engine(PG_DSN, future=True)

    # 부트스트랩
    with engine.begin() as conn:
        conn.execute(text(BOOTSTRAP_SQL))

    types = ("deposit", "saving") if which == "all" else (which,)
    total_ins = total_cls = total_tch = 0

    for t in types:
        pt = PRODUCT_TYPES[t]
        log.info("UPSERT start | product_type=%s", pt)
        with engine.begin() as conn:
            c, i, h = upsert_for_type(conn, pt)
        total_cls += c
        total_ins += i
        total_tch += h
        log.info("UPSERT done  | type=%s | opt_closed=%d, opt_inserted=%d, opt_touched=%d", pt, c, i, h)

    log.info("ALL DONE | opt_closed=%d, opt_inserted=%d, opt_touched=%d", total_cls, total_ins, total_tch)

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    run(arg)