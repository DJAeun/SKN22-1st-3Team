# src/etl/interest/normalize_naver_detail.py

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Tuple, Any

BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
NAVER_DIR = BASE_DIR / "data" / "raw" / "naver"


def normalize_detail(run_id: str) -> Path:
    """
    data/raw/naver/<run_id>/naver_trend_<run_id>.csv 를 읽어서
    model_monthly_interest_detail 테이블에 적재하기 좋은 형태로 정규화된 CSV 생성.

    출력: data/raw/naver/<run_id>/naver_trend_<run_id>_detail_normalized.csv
      - model_id, month, device, gender, age_group, ratio
    """
    raw_path = NAVER_DIR / run_id / f"naver_trend_{run_id}.csv"
    out_path = NAVER_DIR / run_id / f"naver_trend_{run_id}_detail_normalized.csv"

    if not raw_path.exists():
        raise FileNotFoundError(f"네이버 raw CSV를 찾을 수 없습니다: {raw_path}")

    print(f"[INFO] raw CSV 로딩: {raw_path}")

    rows: List[Dict[str, Any]] = []

    with raw_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                model_id = int(row["model_id"])
            except (KeyError, ValueError):
                continue

            date_str = (row.get("date") or "").strip()
            device = (row.get("device") or "").strip()
            gender = (row.get("gender") or "").strip()
            age_group = (row.get("age_group") or "").strip()
            ratio_str = (row.get("ratio") or "").strip()

            if not date_str or not ratio_str:
                continue

            # YYYY-MM-01 로 통일
            if len(date_str) < 7:
                print(f"[WARN] 예기치 않은 날짜 형식 스킵: {date_str}")
                continue
            month = date_str[:7] + "-01"

            try:
                ratio = float(ratio_str)
            except ValueError:
                print(f"[WARN] ratio 파싱 실패 스킵: {ratio_str}")
                continue

            rows.append(
                {
                    "model_id": model_id,
                    "month": month,
                    "device": device,
                    "gender": gender,
                    "age_group": age_group,
                    "ratio": ratio,
                }
            )

    if not rows:
        print("[WARN] 정규화 결과가 비어 있습니다.")
    else:
        print(f"[INFO] 정규화된 레코드 수: {len(rows)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["model_id", "month", "device", "gender", "age_group", "ratio"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"[INFO] 정규화 CSV 저장 완료: {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="네이버 관심도 detail 정규화")
    parser.add_argument("--run-id", required=True, help="수집 실행 ID (예: 25_11_16)")

    args = parser.parse_args()
    normalize_detail(run_id=args.run_id)


if __name__ == "__main__":
    main()
