"""
カンパニーリストテーブルからストックシンボルを一個だけ読み込んで
Yahoo finance から株価を取得し、株価テーブルへ書き込むテストコード
"""

import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3

conn = sqlite3.connect('StockPrice.db')  # データベースにアクセスする。
cursor = conn.cursor()  # カーソルオブジェクト生成
select_sql = """SELECT * FROM CompanyList LIMIT 1"""
cursor.execute(select_sql)
symbol_name = cursor.fetchone()
print(symbol_name[0])

dataSource = 'yahoo'
Symbol1 = symbol_name[0]
start = datetime.datetime(2016, 1, 1)

df = web.DataReader(Symbol1, dataSource, start)  # yahooからダウンロード
df2 = df.resample('M').last()  # 日次データを月次データに変換
# print(df2.index[0])
df2.index = pd.to_datetime(df2.index)
# print(df2.index[0])
StrDate = df2.index.strftime('%Y-%m-%d')  # 時刻除去
# print(str[0])

# 日付の数だけ終値表示
# for date in StrDate:
#     print(date, ',', df2['Adj Close'][date])
# print(StrDate[0])
# print('OK')

for i in StrDate:
    # print(i)
    # print(symbol_name[0], i, df2['Adj Close'][i])
    sql = """INSERT INTO StockPrice VALUES(?, ?, ?)"""  # ?は後で値を受け取るよという意味
    data = (symbol_name[0], i, df2['Adj Close'][i])
    cursor.execute(sql, data)  # executeコマンドでSQL文を実行
    conn.commit()  # コミットする

print('Write OK')

# data = [(symbol_name[0], str[0], df2['Adj Close'][0])]
# print(data)
