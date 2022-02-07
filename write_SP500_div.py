"""
シンボル毎に配当履歴をyahoo financeから配当を読み込んでDBに書き込む
"""
import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time

conn = sqlite3.connect('sp500.db')  # データベースにアクセスする。
cursor1 = conn.cursor()  # カーソルオブジェクト生成(CompanyList)
cursor3 = conn.cursor()  # カーソルオブジェクト生成(Dividends)

# Dividendsテーブルが存在しなければ作成する
create_sql = """CREATE TABLE IF NOT EXISTS Dividends(Symbol, Date, Dividends)"""
cursor3.execute(create_sql)

select_sql = """SELECT * FROM CompanyList"""
# select_sql = """SELECT * FROM CompanyList LIMIT 20"""  # LIMIT句で読み込む個数を指定
cursor1.execute(select_sql)

"""
配当ダウンロード
"""
select_sql = """SELECT * FROM CompanyList"""
# select_sql = """SELECT * FROM CompanyList LIMIT 20"""  # LIMIT句で読み込む個数を指定
cursor1.execute(select_sql)
select_sql = """SELECT DISTINCT Symbol FROM Dividends"""
cursor3.execute(select_sql)

div_list = cursor3.fetchall()  # すでにDividendsテーブルにあるシンボルをリスト化
print('div_list :', div_list, ' ,lem(div_list):', len(div_list))
i = len(div_list) + 1  # すでにあるデータの続きから採番
start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2021, 1, 1)
insert_sql = """INSERT INTO Dividends VALUES(?, ?, ?)"""  # ?は後で値を受け取るよという意味
while True:
    # if i >= 5:
    #     break

    result = cursor1.fetchone()  # データを1行抽出
    print('i=', i, result)
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        print("resultは空です。")
        break  # breakでループ離脱

    if (result[0],) in div_list:
    # if (result[0]) in div_list:
        print(result[0], 'in the div_list!   <<SKIP>>')
        continue

    try:
        # resultはタプル型のため、要素指定して文字列を取り出す必要がある。
        df2 = web.DataReader(result[0], 'yahoo-dividends', start).resample('M').last()
        print('result[0]', result[0], ' result[1]', result[1], ' result[2]', result[2])
        # print(df2)
        # print()
    except TypeError as type_error:
        print(result[0], 'TypeError. CONTINUE')
        continue

    except IndexError as index_error:
        print(result[0], 'Index Error. CONTINUE')
        continue

    print('df2')
    # print(df2)
    StrDate2 = pd.to_datetime(df2.index).strftime('%Y-%m-%d')  # 時刻除去
    print('OK')
    # print(StrDate2)
    for date in StrDate2:
        # print(i)
        # print(symbol_name[0], StrDate[i], df2['Adj Close'][i])
        print(df2['value'][date])
        record2 = (result[0], date, df2['value'][date])
        # print(data)
        cursor3.execute(insert_sql, record2)  # executeコマンドでSQL文を実行
        conn.commit()  # コミットする

    # print("i increment", i, result[0])
    i += 1
    time.sleep(0.5)

print("配当履歴ダウンロード完了")

conn.close()
