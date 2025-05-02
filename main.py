from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

# ✅ 환경변수 로딩
load_dotenv()

app = FastAPI()

# ✅ CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ DB 연결
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("❌ DATABASE_URL 환경변수를 찾을 수 없습니다.")
engine = create_engine(db_url)

# ✅ 1. 자치구 전체 항목 요약 조회 (카페당 기준 포함)
@app.get("/sales/{gu_name}")
def get_sales_summary(gu_name: str):
    query = "SELECT * FROM seoul_sales_summary WHERE 자치구 = %s"
    df = pd.read_sql(query, engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return df.iloc[0].to_dict()

# ✅ 2. 자치구별 카페당 월 평균 매출만 조회
@app.get("/sales/monthly_avg/{gu_name}")
def get_cafe_monthly_avg(gu_name: str):
    query = "SELECT 자치구, 카페당_월_평균_매출 FROM seoul_sales_summary WHERE 자치구 = %s"
    df = pd.read_sql(query, engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return {
        "자치구": df.iloc[0]["자치구"],
        "카페당_월_평균_매출": int(df.iloc[0]["카페당_월_평균_매출"])
    }
