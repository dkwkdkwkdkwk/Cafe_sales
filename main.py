from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware  # ✅ CORS 미들웨어 추가

load_dotenv()

app = FastAPI()

# ✅ CORS 설정: 필요한 경우 allow_origins=["https://your-frontend.com"] 로 제한 가능
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 전체 허용 (보안상 추후 프론트 주소로 제한 권장)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 환경변수에서 DB 연결 문자열 가져오기
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("❌ DATABASE_URL 환경변수를 찾을 수 없습니다.")
engine = create_engine(db_url)

@app.get("/sales/{gu_name}")
def get_sales_summary(gu_name: str):
    query = "SELECT * FROM seoul_sales_summary WHERE 자치구 = %s"
    df = pd.read_sql(query, engine, params=(gu_name,))
    
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    
    return df.iloc[0].to_dict()

# ✅ 자치구별 당월 평균 매출 조회
@app.get("/sales/monthly_avg/{gu_name}")
def get_monthly_avg(gu_name: str):
    query = "SELECT 자치구별_평균_매출 FROM seoul_sales_avg WHERE 자치구 = %s"
    df = pd.read_sql(query, engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 평균 매출 데이터가 없습니다."}
    return {
        "자치구": gu_name,
        "당월_평균_매출": int(df.iloc[0]["자치구별_평균_매출"])
    }
