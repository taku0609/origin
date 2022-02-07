"""
カンパニーリストテーブルからストックシンボルを一個ずつ読み込んで
Yahoo finance から株価を取得し、すべて表示するテストコード
"""

import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time

conn = sqlite3.connect('StockPrice.db')  # データベースにアクセスする。
cursor1 = conn.cursor()  # カーソルオブジェクト生成(CompanyList)
cursor2 = conn.cursor()  # カーソルオブジェクト生成(StockPrice)

# """
# DROP if exists TABLE 削除テーブル名
# """
# sql = """DELETE FROM StockPrice"""
# # 命令を実行
# conn.execute(sql)
# conn.commit()  # コミットする

select_sql = """SELECT Symbol FROM CompanyList"""
cursor1.execute(select_sql)
select_sql = """SELECT DISTINCT Symbol FROM StockPrice"""
cursor2.execute(select_sql)
list = cursor2.fetchall()  # すでにDBにあるシンボルをリスト化
print('list :', list, ' ,lem(list):', len(list))

start = datetime.datetime(2001, 1, 1)
# end = datetime.datetime(2021, 1, 1)
dataSource = 'yahoo'

"""シンボルの数だけ繰り返す"""
i = len(list) + 1  # すでにあるデータの続きから採番
while True:
    # if i == 20:
    #     break

    result = cursor1.fetchone()  # データを1行抽出
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        break  # breakでループ離脱

    if (result[0],) in list:
        print(result[0], 'in the list!   <<SKIP>>')
        continue

    try:
        # resultはタプル型のため、要素指定して文字列を取り出す必要がある。
        df = web.DataReader(result[0], dataSource, start).resample('M').last()
        # print(result[0])
        # print(df)
        # print()
    except Exception as e:
        time.sleep(10.0)
        df = web.DataReader(result[0], dataSource, start).resample('M').last()

    StrDate = pd.to_datetime(df.index).strftime('%Y-%m-%d')  # 時刻除去
    for date in StrDate:
        # print(i)
        # print(symbol_name[0], StrDate[i], df2['Adj Close'][i])
        # ?は後で値を受け取るよという意味
        sql = """INSERT INTO StockPrice VALUES(?, ?, ?, ?, ?, ?, ?, ?)"""
        # dataよりrecordの方がいいかも
        data = (result[0], date, df['Open'][date], df['High'][date],
                df['Low'][date], df['Close'][date], df['Volume'][date], df['Adj Close'][date])
        # print(data)
        cursor2.execute(sql, data)  # executeコマンドでSQL文を実行
        conn.commit()  # コミットする

    print(i, result[0])
    i += 1
    time.sleep(0.5)

print("ダウンロード完了")
conn.close()
