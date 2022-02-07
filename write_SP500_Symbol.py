"""
S&P500のシンボルを取得し、DBに書き込む
https://qiita.com/Fujinoinvestor/items/f4b46d676f3d44646614
"""


import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time

# from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
# codelist = get_nasdaq_symbols()
# print(codelist)

import datapackage

import pandas as pd

# S&P500のシンボルを取得
url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
df = pd.read_csv(url, encoding="SHIFT_JIS")

# print(df)

# データベース名.db拡張子で設定
dbname = ('sp500.db')
# データベースを作成、自動コミット機能ON
conn = sqlite3.connect(dbname, isolation_level=None)

# カーソルオブジェクトを作成
cursor = conn.cursor()

#テーブル削除
delete_sql = """DROP TABLE IF EXISTS CompanyList"""
# # 命令を実行
conn.execute(delete_sql)
conn.commit()  # コミットする

# テーブル作成
create_sql = """CREATE TABLE IF NOT EXISTS CompanyList(Symbol, Name, Sector)"""

# executeコマンドでSQL文を実行
cursor.execute(create_sql)


# データベースにコミット(Excelでいう上書き保存。自動コミット設定なので不要だが一応・・)
conn.commit()

# データベース中のテーブル名を取得するSQL関数
select_sql = """SELECT name FROM sqlite_master WHERE TYPE='table'"""

# for t in cursor.execute(select_sql):  # for文で作成した全テーブルを確認していく
#     print(t)

print()  # 空行
insert_sql = """INSERT INTO CompanyList VALUES(?, ?, ?)"""
for i in df.index:
    # ?は後で値を受け取るよという意味
    # sql = """INSERT INTO CompanyList VALUES(?, ?, ?)"""
    # print(df.index[i], df['Symbol'][i], df['Name'][i], df['Sector'][i])
    record = (df['Symbol'][i], df['Name'][i], df['Sector'][i])
    cursor.execute(insert_sql, record)  # executeコマンドでSQL文を実行
    conn.commit()  # コミットする

# 全レコードを1行ずつ取り出す
select_sql = """SELECT * FROM Companylist"""
cursor.execute(select_sql)

print('DBから取り出して1件ずつ表示')
while True:
    result = cursor.fetchone()  # データを1行抽出
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        break  # breakでループ離脱

    print(result[0], result[1], result[2])

conn.close()
