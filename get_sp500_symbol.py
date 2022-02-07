"""
S&P500のシンボルを取得する
https://qiita.com/Fujinoinvestor/items/f4b46d676f3d44646614
"""

import datetime
import pandas as pd
import pandas_datareader.data as web
import sqlite3
import time

import pandas as pd

url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
df = pd.read_csv(url, encoding="SHIFT_JIS")

print(df)
