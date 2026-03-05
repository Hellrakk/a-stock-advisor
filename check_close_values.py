import pandas as pd
import pickle

df = pickle.load(open("data/akshare_real_data_fixed.pkl", "rb"))
stocks = ["sh601899", "sh600938", "sh600410", "sh601857", "sh600989"]

for stock in stocks:
    stock_data = df[df["stock_code"] == stock]
    if len(stock_data) > 0:
        min_close = stock_data["close"].min()
        max_close = stock_data["close"].max()
        print(f"{stock}: min_close={min_close}, max_close={max_close}, count={len(stock_data)}")
    else:
        print(f"{stock}: No data found")
