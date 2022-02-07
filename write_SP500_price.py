"""
シンボル毎にyahoo financeから株価をひとつずつ読み込んでDBに書き込む
"""
import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time

conn = sqlite3.connect('sp500.db')  # データベースにアクセスする。
cursor1 = conn.cursor()  # カーソルオブジェクト生成(CompanyList)
cursor2 = conn.cursor()  # カーソルオブジェクト生成(StockPrice)

# StockPriceテーブルが存在しなければ作成する
create_sql = """CREATE TABLE IF NOT EXISTS StockPrice(Symbol, Date, Adj)"""
cursor2.execute(create_sql)

select_sql = """SELECT * FROM CompanyList"""
# select_sql = """SELECT * FROM CompanyList LIMIT 20"""  # LIMIT句で読み込む個数を指定
cursor1.execute(select_sql)
select_sql = """SELECT DISTINCT Symbol FROM StockPrice"""
cursor2.execute(select_sql)
list = cursor2.fetchall()  # すでにDBにあるシンボルをリスト化
print('list :', list, ' ,lem(list):', len(list))

"""
株価ダウンロード シンボルの数だけ繰り返す
"""
i = len(list) + 1  # すでにあるデータの続きから採番
start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2021, 1, 1)
insert_sql = """INSERT INTO StockPrice VALUES(?, ?, ?)"""  # ?は後で値を受け取るよという意味
while True:
    # if i == 5:
    #     break

    result = cursor1.fetchone()  # データを1行抽出
    print('i=', i, result)
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        print("resultは空です。")
        break  # breakでループ離脱

    if (result[0],) in list:
        print(result[0], 'in the list!   <<SKIP>>')
        continue

    try:
        # resultはタプル型のため、要素指定して文字列を取り出す必要がある。
        df = web.DataReader(result[0], 'yahoo', start).resample('M').last()
        # print(result[0])
        # print(df)
        # print()
    except Exception as e:
        time.sleep(10.0)

        # 月単位,最終日の情報を取得
        df = web.DataReader(result[0], 'yahoo', start).resample('M').last()

    StrDate = pd.to_datetime(df.index).strftime('%Y-%m-%d')  # 時刻除去
    for date in StrDate:
        # print(i)
        # print(symbol_name[0], StrDate[i], df2['Adj Close'][i])
        record = (result[0], date, df['Adj Close'][date])
        # print(data)
        cursor2.execute(insert_sql, record)  # executeコマンドでSQL文を実行
        conn.commit()  # コミットする

    # print("i increment", i, result[0])
    i += 1
    time.sleep(0.5)

print("株価ダウンロード完了\n")
i = 0

conn.close()
