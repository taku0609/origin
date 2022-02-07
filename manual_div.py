"""
yahoo financeから任意のシンボルの配当データを個別にダウンロードしDBに書き込む
"""
import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time

conn = sqlite3.connect('sp500.db')  # データベースにアクセスする。
cursor3 = conn.cursor()  # カーソルオブジェクト生成(Dividends)

symbol_name = 'CTRA'
start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2021, 1, 1)
insert_sql = """INSERT INTO Dividends VALUES(?, ?, ?, ?)"""  # ?は後で値を受け取るという意味

df = web.DataReader(symbol_name, 'yahoo-dividends', start)
# print('Web.DataReader processing...')
# print(df)

StrDate2 = pd.to_datetime(df.index).strftime('%Y-%m-%d')  # 時刻除去
time.sleep(0.5)

for date in StrDate2:
    try:
        time.sleep(0.5)
        price_df = web.DataReader(symbol_name, 'yahoo', date, date)
    except Exception:
        print('データ取得再試行')
        time.sleep(10.0)
        continue

    div_yield = df['value'][date] / price_df['Adj Close'][date]  # 配当利回り
    record2 = (symbol_name, date, df['value'][date], div_yield)  # シンボル,日付,配当,利回り
    print(record2)
    # print('配当利回り', div_yield)
    cursor3.execute(insert_sql, record2)  # executeコマンドでSQL文を実行

    conn.commit()  # コミットする

print("配当履歴ダウンロード完了")

conn.close()
