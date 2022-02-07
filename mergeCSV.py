"""
2つのCSVをマージし、株価成長率と年間配当利回りを加算する
"""

import csv

f1 = open('C:/Users/4dustd/Desktop/MergeSP500.csv', 'w')
writer = csv.writer(f1, lineterminator='\n')
# ヘッダー
writer.writerow(['Symbol', 'Company Name', 'Start', 'End', '平均成長率(年間)(%)', '平均配当利回り(年間)(%)', '総合リターン(%)'])

f2 = open('C:/Users/4dustd/Desktop/WriteSP500.csv', 'r')
# reader1 = next(csv.reader(f2))
reader1 = csv.reader(f2)
header1 = next(reader1)

f3 = open('C:/Users/4dustd/Desktop/SP500_Dividends&Yields.csv', 'r')
# reader2 = next(csv.reader(f3))
reader2 = csv.reader(f3)
header2 = next(reader2)

"""
CSVオブジェクトは読み込んだあと、カーソルを先頭に戻す必要があり面倒なため、配列に移し替える
"""
sp500_price = []
sp500_div = []

for row1 in reader1:
    sp500_price.append(row1)

# print('SP500_price 移し替え後')
# # print(sp500_price)
# for num in sp500_price:
#     print(num[0])

for row2 in reader2:
    sp500_div.append(row2)

# print('SP500_div 移し替え後')
# for num2 in sp500_div:
#     print(num2[0])

# print('\n2回目')
# for num2 in sp500_div:
#     print(num2[0])

"""
リスト照合
"""
count = 0
for row1 in sp500_price:
    print(row1[0])
    for row2 in sp500_div:
        if row1[0] == row2[0]:
            print(row1[0], 'と', row2[0], 'が一致。')
            print(row2[5])
            if row2[5] != 'null':
                # print(row1[5])
                # print(row2[5])
                # Symbol, Company_Name, Start, End, 平均成長率(年間), 平均配当利回り(年間), 総合リターン
                writer.writerow([row1[0], row1[1], row1[2], row1[3], format(float(row1[5])*100, '.2f'),
                                 format(float(row2[5]), '.2f'), format(float(row1[5])*100 + float(row2[5]), '.2f')])
            else:
                writer.writerow([row1[0], row1[1], row1[2], row1[3], format(float(row1[5])*100, '.2f'),
                                 'null', format(float(row1[5])*100, '.2f')])
        else:
            flag = 1
    if flag is True:
        count += 1

print('一致しなかった個数', count)






