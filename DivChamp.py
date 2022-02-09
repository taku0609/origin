"""
DividendChampionsシートからシンボルを読み込み、yahoo financeから株価と配当履歴をダウンロードする
"""

import openpyxl
import csv
import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time
from pandas_datareader._utils import RemoteDataError

# ブックを取得
excel_path = 'C:/Users/4dustd/Desktop/U.S.DividendChampions.xlsx'
book1 = openpyxl.load_workbook(filename=excel_path)
# シートを取得
sheet1 = book1['Champions']
# セルを取得
temp_list1 = []

sheet_range = sheet1['A4':'AN132']
i = 0
for row1 in sheet_range:
    j = 0
    temp_list2 = []  # 毎回宣言
    for cell in row1:
        # 該当セルの値取得
        cell_value = cell.value
        # 該当セルに値が存在する場合表示
        if cell_value is not None:
            print(cell.coordinate, cell_value)
            # temp_list2[j] = cell_value のように存在しない要素にアクセスしてはいけない。list.append()を使う。
            temp_list2.append(cell_value)  # クリア後、再度宣言
            j += 1
    temp_list1.append(temp_list2)
    # temp_list2.clear()  # 配列の要素クリア
    i += 1

print('\n移し替え後')
for row2 in temp_list1:
    print(row2[0].replace('.', '-'))

"""
Excelから読みだしたシンボルを用い、yahoo financeから株価ダウンロード
"""
conn = sqlite3.connect('DividendChamps.db')  # データベースにアクセスする。
cursor = conn.cursor()  # カーソルオブジェクト生成(Champions)
# StockPriceテーブルが存在しなければ作成する
create_sql = """CREATE TABLE IF NOT EXISTS Champions(Symbol, Date, Adj)"""
cursor.execute(create_sql)
start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2021, 1, 1)
insert_sql = """INSERT INTO Champions VALUES(?, ?, ?)"""
k = 0
special_symbol = {1: "ARTN.A", 2: "MKC.V"}  # これらのシンボルのときに置換する
for row2 in temp_list1:
    # if k == 5:
    #     break

    # match case文 python 3.10
    match row2[0]:
        case 'ARTN.A':
            row2[0] = 'ARTNA'
        case 'MKC.V':
            row2[0] = 'MKC.VI'

    try:
        # 月単位,最終日の情報を取得
        df = web.DataReader(row2[0].replace('.', '-'), 'yahoo', start).resample('M').last()
        # print(result[0])
        # print(df)
        # print()
        """
        remoteErrorをきゃっちする
        """
    except Exception as e:
        time.sleep(10.0)
        try:
            # 月単位,最終日の情報を取得
            df = web.DataReader(row2[0].replace('.', '-'), 'yahoo', start).resample('M').last()
        except RemoteDataError:  # アクセスできないときはスキップする。
            print('remote error:', row2[0])
            continue

    StrDate = pd.to_datetime(df.index).strftime('%Y-%m-%d')  # 時刻除去
    for date in StrDate:
        # print(i)
        print(row2[0].replace('.', '-'), date, df['Adj Close'][date])
        record = (row2[0].replace('.', '-'), date, df['Adj Close'][date])
        # print(data)
        cursor.execute(insert_sql, record)  # executeコマンドでSQL文を実行
        conn.commit()  # コミットする

    # print("i increment", i, result[0])
    k += 1
    time.sleep(0.5)  # 連速でアクセスするとエラーになるためsleepを挟む。

print("株価ダウンロード完了\n")

conn.close()

# ロードしたExcelファイルを閉じる
book1.close()
