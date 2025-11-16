# src/etl/interest/run_naver_trend_crawl.py

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import List, Optional, Tuple

from sqlalchemy import text

from src.api.naver_datalab import NaverDatalabClient
from src.db.connection import get_engine


BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트
NAVER_RAW_BASE = BASE_DIR / "data" / "raw" / "naver"


def fetch_target_models(brands: List[str]) -> List[dict]:
    """
    car_model 테이블에서 네이버 관심도 수집 대상 모델 목록을 조회한다.
    현재는 brand_name IN ('현대', '기아') 기준으로 필터링.
    """
    engine = get_engine(echo=False)

    placeholders = ", ".join([f":b{i}" for i in range(len(brands))])
    params = {f"b{i}": brand for i, brand in enumerate(brands)}

    sql = text(
        f"""
        SELECT
            model_id,
            brand_name,
            model_name_kr
        FROM car_model
        WHERE brand_name IN ({placeholders})
        ORDER BY brand_name, model_name_kr
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    models = [
        {
            "model_id": row["model_id"],
            "brand_name": row["brand_name"],
            "model_name_kr": row["model_name_kr"],
        }
        for row in rows
    ]

    return models


def run_naver_trend_crawl(
    run_id: str,
    start_date: str,
    end_date: str,
    time_unit: str = "month",
    brands: Optional[List[str]] = None,
    sleep_sec: float = 0.3,
    limit_models: Optional[int] = None,
) -> None:
    """
    car_model 기준으로 현대/기아 모델의 네이버 검색 트렌드를 수집하여
    /data/raw/naver/<run_id>/naver_trend_<run_id>.csv 에 저장한다.

    이번 버전은 디바이스/성별 단위까지 상세히 수집:
      - device: pc / mobile
      - gender: male / female
      - age_group: 현재는 필터 미사용 → 빈 문자열로 기록
    """
    if brands is None:
        brands = ["현대", "기아"]

    print(
        f"[INFO] 네이버 데이터랩 수집 시작: run_id={run_id}, 기간={start_date} ~ {end_date}, time_unit={time_unit}"
    )
    print(f"[INFO] 대상 브랜드: {brands}")

    models = fetch_target_models(brands)
    if limit_models is not None:
        models = models[:limit_models]

    if not models:
        print("[WARN] 대상 모델이 없습니다. car_model 테이블을 확인하세요.")
        return

    print(f"[INFO] 수집 대상 모델 수: {len(models)}")

    client = NaverDatalabClient()

    # 디바이스/성별 조합 정의
    # 네이버에 보낼 코드와 CSV에 저장할 라벨을 분리
    device_options: List[Tuple[Optional[str], str]] = [
        ("pc", "pc"),
        ("mo", "mobile"),
    ]
    gender_options: List[Tuple[Optional[str], str]] = [
        ("m", "male"),
        ("f", "female"),
    ]

    # 출력 디렉토리 및 파일 준비
    out_dir = NAVER_RAW_BASE / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"naver_trend_{run_id}.csv"

    fieldnames = [
        "model_id",
        "brand_name",
        "model_name",
        "date",
        "device",
        "gender",
        "age_group",
        "ratio",
    ]

    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        total_calls = 0

        for idx, m in enumerate(models, start=1):
            model_id = m["model_id"]
            brand_name = m["brand_name"]
            model_name = m["model_name_kr"]

            print(
                f"[INFO] ({idx}/{len(models)}) 모델 처리 중: [{brand_name}] {model_name} (model_id={model_id})"
            )

            for device_code, device_label in device_options:
                for gender_code, gender_label in gender_options:
                    try:
                        data_points = client.fetch_trend(
                            keyword=model_name,
                            start_date=start_date,
                            end_date=end_date,
                            time_unit=time_unit,
                            device=device_code,
                            gender=gender_code,
                            ages=None,  # 나이 필터는 지금은 사용하지 않음
                        )
                        total_calls += 1
                    except Exception as e:
                        print(
                            f"[WARN] 네이버 API 호출 실패: "
                            f"[{brand_name}] {model_name}, device={device_code}, gender={gender_code}, error={e}"
                        )
                        continue

                    if not data_points:
                        print(
                            f"[WARN] 네이버 데이터 없음: "
                            f"[{brand_name}] {model_name}, device={device_code}, gender={gender_code}"
                        )
                        continue

                    for dp in data_points:
                        period = dp.get("period")
                        ratio = dp.get("ratio")
                        if period is None or ratio is None:
                            continue

                        writer.writerow(
                            {
                                "model_id": model_id,
                                "brand_name": brand_name,
                                "model_name": model_name,
                                "date": period,
                                "device": device_label,
                                "gender": gender_label,
                                "age_group": "",  # 추후 ages 사용 시 여기 채우면 됨
                                "ratio": ratio,
                            }
                        )

                    if sleep_sec > 0:
                        time.sleep(sleep_sec)

    print(f"[INFO] 네이버 데이터랩 수집 완료: {out_path}")
    print(f"[INFO] 총 API 호출 수: {total_calls}")


def main():
    parser = argparse.ArgumentParser(
        description="네이버 데이터랩 관심도 수집 (car_model 기준, device×gender 상세)"
    )
    parser.add_argument("--run-id", required=True, help="수집 실행 ID (예: 25_11_16)")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD 형식 시작일")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD 형식 종료일")
    parser.add_argument(
        "--time-unit",
        choices=["date", "week", "month"],
        default="month",
        help="네이버 데이터랩 timeUnit (기본: month)",
    )
    parser.add_argument(
        "--brands",
        nargs="+",
        default=["현대", "기아"],
        help="대상 브랜드명 목록 (car_model.brand_name 기준, 기본: 현대 기아)",
    )
    parser.add_argument(
        "--limit-models",
        type=int,
        default=None,
        help="테스트용: 상위 N개 모델만 수집",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.3,
        help="모델×필터 조합별 API 호출 사이 딜레이(초)",
    )

    args = parser.parse_args()

    run_naver_trend_crawl(
        run_id=args.run_id,
        start_date=args.start_date,
        end_date=args.end_date,
        time_unit=args.time_unit,
        brands=args.brands,
        sleep_sec=args.sleep_sec,
        limit_models=args.limit_models,
    )


if __name__ == "__main__":
    main()
