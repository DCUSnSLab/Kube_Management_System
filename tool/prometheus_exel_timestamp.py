import pandas as pd
import matplotlib.pyplot as plt

# CSV 파일 경로
csv_path = "CPU Usage-data-as-joinbyfield-2025-07-27 20_58_17.csv"

# 데이터 로드 및 전처리
df = pd.read_csv(csv_path)
df['Time'] = pd.to_datetime(df['Time'])

# NaN 값은 0으로 변환 후 전체 합계 계산
df['cpu_total'] = df.drop(columns='Time').fillna(0).sum(axis=1)

# 날짜별로 집계
df['Date'] = df['Time'].dt.date
daily_usage = df.groupby('Date')['cpu_total'].sum().reset_index()

# 그래프 그리기
plt.figure(figsize=(15, 5))
plt.plot(daily_usage['Date'], daily_usage['cpu_total'], color='cyan')
plt.title("Total CPU Usage per Day")
plt.xlabel("Date")
plt.ylabel("Total CPU Usage")
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
