# src/etl/interest/load_naver_interest_detail.py

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Any

from sqlalchemy import text

from src.db.connection import get_engine


BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
NAVER_DIR = BASE_DIR / "data" / "raw" / "naver"


def load_detail(run_id: str):
    """
    정규화된 네이버 detail CSV를 읽어서
    model_monthly_interest_detail 테이블에 upsert.
    """
    csv_path = NAVER_DIR / run_id / f"naver_trend_{run_id}_detail_normalized.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"정규화된 detail CSV가 없습니다: {csv_path}")

    print(f"[INFO] 로딩 시작: {csv_path}")

    engine = get_engine(echo=False)

    sql = text(
        """
        INSERT INTO model_monthly_interest_detail (
            model_id,
            month,
            device,
            gender,
            age_group,
            ratio,
            created_at
        )
        VALUES (
            :model_id,
            :month,
            :device,
            :gender,
            :age_group,
            :ratio,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            ratio = VALUES(ratio)
        """
    )

    rows = 0

    with engine.begin() as conn:
        with csv_path.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                params = {
                    "model_id": int(row["model_id"]),
                    "month": row["month"],
                    "device": row["device"] or None,
                    "gender": row["gender"] or None,
                    "age_group": row["age_group"] or None,
                    "ratio": float(row["ratio"]),
                }
                conn.execute(sql, params)
                rows += 1

    print(f"[INFO] detail 테이블 upsert 완료: {rows} rows")


def main():
    parser = argparse.ArgumentParser(description="네이버 관심도 detail 로더")
    parser.add_argument("--run-id", required=True, help="수집 실행 ID (예: 25_11_16)")
    args = parser.parse_args()

    load_detail(run_id=args.run_id)


if __name__ == "__main__":
    main()
