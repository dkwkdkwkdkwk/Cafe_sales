
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ✅ 환경변수 로딩
load_dotenv()

# ✅ CSV 파일 불러오기
df = pd.read_csv("서울시매출데이터.csv", encoding="cp949")

# ✅ 자치구별 항목 평균 계산 (남녀, 연령대, 요일, 시간대)
summary_rows = []
for gu in df["자치구"].unique():
    df_gu = df[df["자치구"] == gu]
    
    row = {
        "자치구": gu,

        # 성별 평균
        "남성": df_gu["남성_매출_금액"].mean(),
        "여성": df_gu["여성_매출_금액"].mean(),

        # 연령대별 평균
        "연령_10": df_gu["연령대_10_매출_금액"].mean(),
        "연령_20": df_gu["연령대_20_매출_금액"].mean(),
        "연령_30": df_gu["연령대_30_매출_금액"].mean(),
        "연령_40": df_gu["연령대_40_매출_금액"].mean(),
        "연령_50": df_gu["연령대_50_매출_금액"].mean(),
        "연령_60": df_gu["연령대_60_이상_매출_금액"].mean(),

        # 요일별 평균
        "월": df_gu["월요일_매출_금액"].mean(),
        "화": df_gu["화요일_매출_금액"].mean(),
        "수": df_gu["수요일_매출_금액"].mean(),
        "목": df_gu["목요일_매출_금액"].mean(),
        "금": df_gu["금요일_매출_금액"].mean(),
        "토": df_gu["토요일_매출_금액"].mean(),
        "일": df_gu["일요일_매출_금액"].mean(),

        # 시간대별 평균
        "00_06": df_gu["시간대_00~06_매출_금액"].mean(),
        "06_11": df_gu["시간대_06~11_매출_금액"].mean(),
        "11_14": df_gu["시간대_11~14_매출_금액"].mean(),
        "14_17": df_gu["시간대_14~17_매출_금액"].mean(),
        "17_21": df_gu["시간대_17~21_매출_금액"].mean(),
        "21_24": df_gu["시간대_21~24_매출_금액"].mean()
    }

    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)

# ✅ 자치구별 당월_매출_금액 평균
avg_sales_df = df.groupby("자치구")["당월_매출_금액"].mean().reset_index()
avg_sales_df.rename(columns={"당월_매출_금액": "자치구별_평균_매출"}, inplace=True)

# ✅ DB 저장
db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)
summary_df.to_sql("seoul_sales_summary", con=engine, if_exists="replace", index=False)
avg_sales_df.to_sql("seoul_sales_avg", con=engine, if_exists="replace", index=False)

print("✅ 자치구별 항목 평균 및 월평균 매출 저장 완료")
