import pandas as pd
from sqlalchemy import create_engine

# 데이터 불러오기
df = pd.read_csv("서울시매출데이터.csv", encoding="cp949")

# 자치구별 요약 데이터 생성
summary_rows = []
for gu in df["자치구"].unique():
    df_gu = df[df["자치구"] == gu]
    
    row = {
        "자치구": gu,
        "남성": df_gu["남성_매출_금액"].sum(),
        "여성": df_gu["여성_매출_금액"].sum(),
        "연령_10": df_gu["연령대_10_매출_금액"].sum(),
        "연령_20": df_gu["연령대_20_매출_금액"].sum(),
        "연령_30": df_gu["연령대_30_매출_금액"].sum(),
        "연령_40": df_gu["연령대_40_매출_금액"].sum(),
        "연령_50": df_gu["연령대_50_매출_금액"].sum(),
        "연령_60": df_gu["연령대_60_이상_매출_금액"].sum(),
        "월": df_gu["월요일_매출_금액"].sum(),
        "화": df_gu["화요일_매출_금액"].sum(),
        "수": df_gu["수요일_매출_금액"].sum(),
        "목": df_gu["목요일_매출_금액"].sum(),
        "금": df_gu["금요일_매출_금액"].sum(),
        "토": df_gu["토요일_매출_금액"].sum(),
        "일": df_gu["일요일_매출_금액"].sum(),
        "00_06": df_gu["시간대_00~06_매출_금액"].sum(),
        "06_11": df_gu["시간대_06~11_매출_금액"].sum(),
        "11_14": df_gu["시간대_11~14_매출_금액"].sum(),
        "14_17": df_gu["시간대_14~17_매출_금액"].sum(),
        "17_21": df_gu["시간대_17~21_매출_금액"].sum(),
        "21_24": df_gu["시간대_21~24_매출_금액"].sum()
    }
    summary_rows.append(row)

# 데이터프레임 변환 및 저장
summary_df = pd.DataFrame(summary_rows)
pw_encoded = "Sangmyung1234%21%21"
engine = create_engine(f"mysql+pymysql://root:{pw_encoded}@localhost:3306/sales_pj")
summary_df.to_sql("seoul_sales_summary", con=engine, if_exists="replace", index=False)

print("✅ MySQL에 저장 완료")
