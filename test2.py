import datetime

import pandas as pd
import pandas_datareader.data as web

import matplotlib.pyplot as plt

# with open('data/temp/alpha_vantage_api_key.txt') as f:
#     api_key = f.read()

start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2021, 1, 1)

dataSource = 'yahoo'

Symbol1 = 'AAPL'
df_sne = web.DataReader(Symbol1, dataSource, start)
print(df_sne)
# print(df_sne['Adj Close'][0])
df_sne2 = df_sne.resample('M').last()  # 日次データを月次データに変換
# df_sne.index = pd.to_datetime(df_sne.index)
df_sne2.index = pd.to_datetime(df_sne2.index)
# print(df_sne.index[0], df_sne['Adj Close'][0])
# print(df_sne.index)
# print(df_sne2.index)

# print(df_sne2.index[0])
for date in df_sne2.index:
    print(date, ' ', df_sne2['Adj Close'][date])

# Symbol2 = 'AAPL'
# df_aapl = web.DataReader(Symbol2, dataSource, start, end)
# print(df_aapl)
#
# df_adjclose = pd.DataFrame({Symbol1: df_sne['Adj Close'], Symbol2: df_aapl['Adj Close']})
# print(df_adjclose)

#
# # print(type(df_sne.index))
#
# df_adjclose.index = pd.to_datetime(df_adjclose.index)
# # print(type(df_sne.index))
#
# df_adjclose.plot(title='SNE vs. AAPL', grid=True)
# plt.show()

# plt.close()
