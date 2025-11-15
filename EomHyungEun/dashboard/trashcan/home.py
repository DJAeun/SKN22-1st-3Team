import streamlit as st
import pandas as pd

# components
from components.inputs import model_selectbox, year_select
from components.images import image_card
from components.charts import line_chart, bar_chart, pie_chart
from components.kpi import kpi_row
from components.layout import two_columns_ratio, three_columns

st.set_page_config(page_title="자동차 시장 대시보드", layout="wide")

st.title("🚗 자동차 시장 개요 Dashboard")

# ------------------------------
# 📌 입력 컴포넌트
# ------------------------------
st.subheader("📌 분석 조건 선택")

col1, col2 = two_columns_ratio(2, 1)

with col1:
    model = model_selectbox("모델 선택", ["쏘렌토", "카니발", "스포티지", "셀토스"])
with col2:
    year = year_select("연도 선택")

st.write(f"### 🔎 선택한 모델: **{model}**, 연도: **{year}**")

# ------------------------------
# 📌 KPI 카드
# ------------------------------
st.subheader("📊 핵심 지표")

kpi_row({
    "총 판매량": ("43,210대", "+4.2%"),
    "시장 점유율": ("12.8%", "+0.7%"),
    "경쟁 모델 수": (7, None),
})

# ------------------------------
# 📌 이미지 카드
# ------------------------------
st.subheader("🚘 모델 이미지")

image_card(
    title=f"{model} 대표 이미지",
    image_url="https://picsum.photos/900/500",
    caption=f"{model}의 예시 이미지입니다."
)

# ------------------------------
# 📌 차트 데이터 생성 (예시 데이터)
# ------------------------------
df = pd.DataFrame({
    "month": pd.date_range(f"{year}-01-01", periods=12, freq="M"),
    "sales": [3000, 3200, 3100, 3300, 3500, 3400, 3600, 3800, 4000, 4200, 4100, 4300]
})

market_df = pd.DataFrame({
    "model": ["쏘렌토", "카니발", "스포티지", "셀토스"],
    "share": [28, 24, 22, 26]
})

# ------------------------------
# 📌 레이아웃 + 차트
# ------------------------------
st.subheader("📈 판매량 분석")

chart_col1, chart_col2 = two_columns_ratio(2, 1)

with chart_col1:
    st.write("### 📈 월별 판매 추이")
    line_chart(df, x="month", y="sales", title=f"{model} 판매 추이")

with chart_col2:
    st.write("### 🥧 시장 점유율")
    pie_chart(market_df, names="model", values="share", title="모델별 시장 점유율")

# ------------------------------
# 📌 바 차트 (추가)
# ------------------------------
st.write("### 📊 연간 판매 명세")

bar_chart(df, x="month", y="sales", title=f"{year}년 월별 판매")

st.success("✅ 대시보드 렌더링 완료!")
