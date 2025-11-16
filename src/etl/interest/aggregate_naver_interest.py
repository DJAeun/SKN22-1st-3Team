# src/etl/interest/aggregate_naver_interest.py

from __future__ import annotations

import argparse
from typing import List, Dict, Any

from sqlalchemy import text

from src.db.connection import get_engine


def fetch_aggregated_naver_index() -> List[Dict[str, Any]]:
    """
    model_monthly_interest_detail 에서
    (model_id, month) 단위로 평균 ratio 를 집계해온다.
    """
    engine = get_engine(echo=False)

    sql = text(
        """
        SELECT
            model_id,
            month,
            AVG(ratio) AS naver_index
        FROM model_monthly_interest_detail
        GROUP BY model_id, month
        ORDER BY month, model_id
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    return [
        {
            "model_id": row["model_id"],
            "month": row["month"],
            "naver_index": (
                float(row["naver_index"]) if row["naver_index"] is not None else None
            ),
        }
        for row in rows
    ]


def upsert_model_monthly_interest(aggregated: List[Dict[str, Any]]) -> None:
    """
    집계된 네이버 관심도(aggregated)를
    model_monthly_interest 테이블에 upsert 한다.

    - DB 컬럼명:
        naver_search_index       ← 네이버 검색 지수
        google_trend_index       ← (미사용, NULL 유지)
        danawa_pop_rank          ← (미사용, NULL 유지)
        danawa_pop_rank_size     ← (미사용, NULL 유지)
    """
    if not aggregated:
        print("[WARN] 집계된 데이터가 없습니다. detail 테이블을 확인하세요.")
        return

    engine = get_engine(echo=False)

    sql = text(
        """
        INSERT INTO model_monthly_interest (
            model_id,
            month,
            naver_search_index,
            google_trend_index,
            danawa_pop_rank,
            danawa_pop_rank_size,
            created_at
        )
        VALUES (
            :model_id,
            :month,
            :naver_search_index,
            NULL,
            NULL,
            NULL,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            naver_search_index = VALUES(naver_search_index)
        """
    )

    with engine.begin() as conn:
        for row in aggregated:
            if row["naver_index"] is None:
                continue

            conn.execute(
                sql,
                {
                    "model_id": row["model_id"],
                    "month": row["month"],
                    "naver_search_index": row["naver_index"],
                },
            )

    print(f"[INFO] model_monthly_interest upsert 완료 (rows={len(aggregated)})")


def run_aggregate() -> None:
    print("[INFO] 네이버 detail → model_monthly_interest 집계 시작")
    aggregated = fetch_aggregated_naver_index()
    print(f"[INFO] 집계된 (model_id, month) 개수: {len(aggregated)}")
    upsert_model_monthly_interest(aggregated)
    print("[INFO] 네이버 관심도 집계 완료")


def main():
    parser = argparse.ArgumentParser(
        description="model_monthly_interest_detail → model_monthly_interest (naver_search_index 집계)"
    )
    args = parser.parse_args()

    run_aggregate()


if __name__ == "__main__":
    main()
