from fastapi import FastAPI 
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv  # 추가

load_dotenv()  # 추가

app = FastAPI()

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
