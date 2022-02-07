"""
DBから配当データを取り出し、CSVに書き込んでいく
配当って年に何回？
４倍しないといけないかも
"""

import sqlite3
import csv

# CSV読み込み
f = open('C:/Users/4dustd/Desktop/SP500_Dividends&Yields.csv', 'w')
writer = csv.writer(f, lineterminator='\n')
writer.writerow(['Symbol', 'Company Name', 'Start', 'End', '配当$(年間)', '利回り%(年間)'])  # ヘッダー

conn = sqlite3.connect('SP500.db')  # データベースにアクセスする。

# カーソルオブジェクト生成
cursor1 = conn.cursor()
cursor2 = conn.cursor()  # Dividends

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
    # 配当がない企業もある。該当なしならnullをCSVに書き込む
    try:
        # select_sql = """SELECT * FROM Dividends WHERE Dividends.Symbol = ? ORDER BY Date ASC"""
        select_sql = """SELECT * FROM Dividends WHERE Symbol = ? ORDER BY Date ASC"""
        cursor2.execute(select_sql, (result[0],))  # executeの第2引数はタプルで記述
        result2 = cursor2.fetchall()

        # 月利(幾何平均)の算出 (final/first)^(1/n)-1

        # 配当利回りの合計
        div_sum = 0
        yield_sum = 0
        num = 0
        for index in result2:
            print('$', result2[num][2])
            print(result2[num][3])
            div_sum += result2[num][2]
            yield_sum += result2[num][3]
            num += 1

        # 配当利回りの平均
        avg_div = div_sum / len(result2)
        avg_yield = yield_sum / len(result2)

        # gm_month_div = (result2[len(result2)-1][2]/result2[0][2]) ** (1/len(result2))-1

        # print(result2)
        # print(len(result2))
        # print(result2[0][2])
        # gm_annu_div = (1 + gm_month_div) ** 12 - 1
        print('データ数:', len(result2))
        print('配当(平均)', format(avg_div, '.2f'))
        print("年間配当利回り", format(avg_yield*4*100, '.2f'), "%")
        # print(result2[0][1], result2[len(result2) - 1][1])
        # symbol, company name, start, end, dividends, yield
        writer.writerow([result[0], result[1], result2[0][1], result2[len(result2) - 1][1], format(avg_div*4, '.3f'),
                         format(avg_yield*4*100, '.4f')])
        # writer.writerow([result[0], result[1], format(geo_mean_month, '.2f'),
        #                  format(geo_mean_annu, '.2f')])
        f.flush()

    except TypeError:
        print('Yield NULL')
        # symbol, 社名, 開始, 終了, null, null
        writer.writerow([result[0], result[1], 'null', 'null', 'null', 'null'])
        f.flush()
        print('IndexError Continue')
        # continue

    # i += 1
    # if i == 20:
    #     break

# ファイルクローズ
f.close()

# DBクローズ
conn.close()
