import sqlite3
import pandas as pd

# データベース名.db拡張子で設定
dbname = ('test.db')
#dbname = ('test.sqlite3')
# データベースを作成、自動コミット機能ON
conn = sqlite3.connect(dbname, isolation_level=None)

# カーソルオブジェクトを作成
cursor = conn.cursor()

"""
・create table テーブル名（作成したいデータカラム）というSQL文でテーブルを宣言
　　※SQL命令は大文字でも小文字でもいい
・今回はtestテーブルに「id,name,date」カラム(列名称)を定義する※今回dateは生年月日という列
・「if not exists」はエラー防止の部分。すでに同じテーブルが作成されてるとエラーになる為
・カラム型は指定しなくても特には問題ない
　　※NULL, INTEGER(整数), REAL(浮動小数点), TEXT(文字列), BLOB(バイナリ)の5種類
"""
sql = """CREATE TABLE IF NOT EXISTS test(id, name, date)"""

# executeコマンドでSQL文を実行
cursor.execute(sql)
# データベースにコミット(Excelでいう上書き保存。自動コミット設定なので不要だが一応・・)
conn.commit()

# データベース中のテーブル名を取得するSQL関数
sql = """SELECT name FROM sqlite_master WHERE TYPE='table'"""

for t in cursor.execute(sql):  # for文で作成した全テーブルを確認していく
    print(t)

"""
レコードを追加する場合はinsert文を使う。
SQLインジェクションという不正SQL命令への脆弱性対策でpythonの場合は「？」を使用して記載するのが基本。
"""

sql = """INSERT INTO test VALUES(?, ?, ?)"""  # ?は後で値を受け取るよという意味

data = [
    (1, "Taro", 19800810),
    (2, "Bob", 19921015),
    (3, "Masa", 20050505),
    (4, "Jiro", 19910510),
    (5, "Satoshi", 19880117)
]
cursor.executemany(sql, data)  # 複数のデータを追加したい場合はexecutemanyメソッドを使う

# data = ((1, 'Taro', 19800810))  # 挿入するレコードを指定
# cursor.execute(sql, data)  # executeコマンドでSQL文を実行
conn.commit()  # コミットする

"""
select * ですべてのデータを参照し、fromでどのテーブルからデータを呼ぶのか指定
fetchallですべての行のデータを取り出す
"""
sql = """SELECT * FROM test"""
cursor.execute(sql)
print(cursor.fetchall())  # 全レコードを取り出す

# 全レコードを1行ずつ取り出す
select_sql = """SELECT * FROM test"""
cursor.execute(select_sql)

while True:
    result = cursor.fetchone()  # データを1行抽出
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        break  # breakでループ離脱

    print(result)

# dbをread_sqlを使用してpandasとして読み出す。
df = pd.read_sql('SELECT * FROM test', conn)

print(df.head())

#テーブル削除
"""
DROP if exists TABLE 削除テーブル名
"""
# sql = """DROP TABLE IF EXISTS test"""
#
# # 命令を実行
# conn.execute(sql)
# conn.commit()  # コミットする
#
# # 作業完了したらDB接続を閉じる
# conn.close()
