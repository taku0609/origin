import sqlite3
import pandas as pd

dbfile = sqlite3.connect('StockPrice.db')  # データベースにアクセスする。

cursor = dbfile.cursor()  # カーソルオブジェクト生成

# データベース中のテーブル名を取得するSQL関数
sql = """SELECT name FROM sqlite_master WHERE TYPE='table'"""

for t in cursor.execute(sql):  # for文で作成した全テーブルを確認していく
    print(t)

print()  # 空行

# 全レコードを1行ずつ取り出す
select_sql = """SELECT * FROM Companylist"""
cursor.execute(select_sql)

while True:
    result = cursor.fetchone()  # データを1行抽出
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        break  # breakでループ離脱

    print(result)

dbfile.close()
