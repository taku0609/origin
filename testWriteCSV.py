"""
DBからレコードを取り出しCSVに書き込んでいく
"""

import sqlite3
import csv

# CSV読み込み
f = open('C:/Users/4dustd/Desktop/SP500.csv', 'w')
writer = csv.writer(f, lineterminator='\n')
writer.writerow(['Symbol', 'Company Name', 'Start', 'End', '月間平均成長率', '年間平均成長率'])  # ヘッダー

conn = sqlite3.connect('SP500.db')  # データベースにアクセスする。

# カーソルオブジェクト生成
cursor1 = conn.cursor()
cursor2 = conn.cursor()


# 全レコードを1行ずつ取り出す
select_sql = """SELECT * FROM Companylist"""
cursor1.execute(select_sql)
i = 0
while True:
    result = cursor1.fetchone()  # データを1行抽出
    if result is None:  # ループ離脱条件(データを抽出しきって空になったら)
        break  # breakでループ離脱

    # print(result)
    print('[', result[0], '] ', result[1])  # シンボル, 社名
    select_sql = """SELECT * FROM StockPrice WHERE StockPrice.Symbol = ? ORDER BY Date ASC"""
    cursor2.execute(select_sql, (result[0],))  # executeの第2引数はタプルで記述
    result2 = cursor2.fetchall()
    # print(result2[len(result2) - 1][1], result2[len(result2) - 1][2])
    # print(result2[0][1], result2[0][2])
    # print(len(result2))
    # print((result2[len(result2) - 1][7] / result2[0][7]) ** (1 / len(result2)) - 1)

    try:
        # 月利(幾何平均)の算出 (final/first)^(1/n)-1
        geo_mean_month = (result2[len(result2) - 1][2] / result2[0][2]) ** (1 / len(result2)) - 1

        # print(result2)
        # print(len(result2))
        # print(result2[0][2])
        geo_mean_annu = (1 + geo_mean_month) ** 12 - 1
        print("月利", format(geo_mean_month * 100, '.2f'), "%")
        print("年利", format(geo_mean_annu * 100, '.2f'), "%")
        # print(result2[0][1], result2[len(result2) - 1][1])
        writer.writerow(
            [result[0], result[1], result2[0][1], result2[len(result2) - 1][1], format(geo_mean_month, '.4f'),
             format(geo_mean_annu, '.4f')])
        # writer.writerow([result[0], result[1], format(geo_mean_month, '.2f'),
        #                  format(geo_mean_annu, '.2f')])
        f.flush()
    except Exception as e:
        print('AjtClose NULL')
        writer.writerow([result[0], result[1], result2[0][1], result2[len(result2) - 1][1],
                         'null', 'null'])
        f.flush()



    # i += 1
    # if i == 20:
    #     break

# ファイルクローズ
f.close()

# DBクローズ
conn.close()
