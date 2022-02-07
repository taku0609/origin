"""
シンボル毎に配当履歴をyahoo financeから配当を読み込んで表示する、DBに書き込む

=>RemoteErrorを克服できないため、yfinanceライブラリに移行
"""
import datetime
import sqlite3
import time

import pandas as pd
# import pandas_datareader.data as web
from pandas_datareader import data as pdr
import yfinance as yfin
yfin.pdr_override()

conn = sqlite3.connect('sp500.db')  # データベースにアクセスする。
cursor1 = conn.cursor()  # カーソルオブジェクト生成(CompanyList)
cursor3 = conn.cursor()  # カーソルオブジェクト生成(Dividends)

# symbolテーブルからシンボルを読み込み
select_sql = """SELECT * FROM CompanyList"""
# select_sql = """SELECT * FROM CompanyList LIMIT 5"""  # LIMIT句で読み込む個数を指定
cursor1.execute(select_sql)

# Dividendsテーブルが存在しなければ作成する
create_sql = """CREATE TABLE IF NOT EXISTS Dividends(Symbol, Date, Dividend, Yield)"""
cursor3.execute(create_sql)

"""
配当ダウンロード
"""
select_sql = """SELECT DISTINCT Symbol FROM Dividends"""
cursor3.execute(select_sql)

div_list = cursor3.fetchall()  # すでにDividendsテーブルにあるシンボルをリスト化
# print('div_list :', div_list, ' ,lem(div_list):', len(div_list))
i = len(div_list) + 1  # すでにあるデータの続きから採番
start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2021, 1, 1)
insert_sql = """INSERT INTO Dividends VALUES(?, ?, ?, ?)"""  # ?は後で値を受け取るという意味

while True:
    # if i >= 5:
    #     break

    result = cursor1.fetchone()  # データを1行抽出
    print('i=', i, result)
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        print("resultは空です。")
        break  # breakでループ離脱

    if (result[0],) in div_list:
        print(result[0], 'in the div_list!   <<SKIP>>')
        continue

    # try:
    # resultはタプル型のため、要素指定して文字列を取り出す必要がある。
    # df2 = web.DataReader(result[0], 'yahoo-dividends', start)
    df2 = pdr.get_data_yahoo(result[0], 'yahoo-dividends', start).resample('M')
    # print('result[0]', result[0], ' result[1]', result[1], ' result[2]', result[2])
    if not df2.empty:
        # print('df2.index:', df2.index)
        # StrDate2 = pd.to_datetime(df2.index).strftime('%Y-%m-%d')  # 時刻除去
        # print('時刻除去OK')
        StrDate2 = pd.to_datetime(df2.index)
        # print(StrDate2)
        for date in StrDate2:
            # print(date)
            # print(symbol_name[0], StrDate[i], df2['Adj Close'][i])
            # price_df = web.DataReader(result[0], 'yahoo', date, date).resample('M').last()
            try:
                # price_df = pdr.DataReader(result[0], 'yahoo', date, date)
                price_df = pdr.get_data_yahoo(result[0], 'yahoo', date, date)
            except KeyError:
                print('データ取得再試行')
                time.sleep(10.0)
                continue

            # print(df2['value'][date])
            # print('当日の株価', price_df['Adj Close'][date])
            div_yield = df2['value'][date] / price_df['Adj Close'][date]  # 配当利回り
            record2 = (result[0], date, df2['value'][date], div_yield)  # シンボル,日付,配当,利回り
            # print('配当利回り', div_yield)
            try:
                cursor3.execute(insert_sql, record2)  # executeコマンドでSQL文を実行
            except sqlite3.InterfaceError:
                continue
            conn.commit()  # コミットする
    else:
        print('df2.index is Empty')
        record2 = (result[0], 'null', 'null', 'null')
        cursor3.execute(insert_sql, record2)  # executeコマンドでSQL文を実行
        conn.commit()  # コミットする
    # print()
    # except TypeError as type_error:
    #     print(result[0], 'TypeError. CONTINUE')
    #     continue
    #
    # except IndexError as index_error:
    #     print(result[0], 'Index Error. CONTINUE')
    #     continue

    # print('df2.index:', df2.index)

    # print(df2)

    # print(data)

    # print("i increment", i, result[0])
    i += 1
    time.sleep(0.5)

print("配当履歴ダウンロード完了")

conn.close()
