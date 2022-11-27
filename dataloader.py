# %%
def gather_gsheets():
  from google.colab import auth
  auth.authenticate_user()

  import gspread
  from google.auth import default
  creds, _ = default()

  gc = gspread.authorize(creds)

  worksheet = gc.open('FX Data').sheet1

  # get_all_values gives a list of rows.
  rows = worksheet.get_all_values()
  # print(rows)

  import pandas as pd
  df = pd.DataFrame.from_records(rows, columns=["Instrument", "Local_time", "Date", "Open", "High", "Low", "Close", "Volume", "DateXL", "Returns"])
  df = df[1:]
  return df

if __name__ == "__main__":
  gather_gsheets()

# %%
def gather_econdb():
  # +production and demand for LNG, oil
  import pandas_datareader.data as web
  df = web.DataReader(
      '&'.join([
          'dataset=JODI_GAS', 
          'h=TIME', 
          'v=Energy product', 
          'from=2021-11-01', 
          'to=2022-05-01', 
          'ENERGY_PRODUCT=[NATGAS]',
          'REF_AREA=[US]'
      ]),
      'econdb'
  )
  return df

if __name__ == "__main__":
  gather_econdb()
  
# %%
"""
https://www.myfxbook.com/community/outlook
updated every minute, looks ajax driven with pop up
updated every minute based on myfxbook users
"""

"""
# unrelated zip archive
# https://dev.to/jakewitcher/uploading-and-downloading-zip-files-in-gcp-cloud-storage-using-python-2l1b

# scrapers for trading economics?
# --- retail
# https://www.dailyfx.com/education/beginner/interest-rates-and-forex-market.html
https://www.dailyfx.com/sentiment-report
https://www.dailyfx.com/sentiment

# oml4py
# schedule operations, sync to ADB and occasionally save csv to public cloud storage.
# cloud storage also an idea for storing parameters on a call (Dynatrace?).
# looking at days sentiment, trend following/line breakdown, 
# eg. after max spike and drop in longs
# prices tend to continue long
# updated every 4 hours from 10:03 AM SAST
"""
def gather_sentiment():
  import pandas as pd

  dfs = pd.read_html("https://www.dailyfx.com/sentiment-report")
  # dfs[0].to_html()
  dfs[0].to_csv()

# https://colab.research.google.com/notebooks/snippets/sheets.ipynb#scrollTo=JiJVCmu3dhFa
# https://colab.research.google.com/notebooks/bigquery.ipynb#scrollTo=UMKGkkZEPVRu

# %%
def gather_av(pairs, ALPHAVANTAGE_API, source="av-forex-daily"):
  """
  Sources:
  av-forex-daily
  av-daily
  """
  import pandas_datareader.data as web
  import pandas as pd
  from time import sleep

  # consider candle_date to avoid sql keywords
  sec_arr = []
  for pair in pairs:
    print (pair)
    sec = web.DataReader(pair, source, api_key=ALPHAVANTAGE_API)
    sec["returns"] = sec["close"] - sec["close"].shift(1)
    sec["symbol"] = pair
    sec["date"] = pd.to_datetime(sec.index)
    sec_arr.append(sec)
    sleep(1)
  sec_all = pd.concat(sec_arr)
  return sec_all

if __name__ == "__main__":
  print (
      av_forex(["AUD/JPY", "NZD/JPY", "GBP/JPY"])
  )
  
# %%
def get_drive_csv(pairs, resolution="Hour"):
  import pandas as pd

  from pathlib import Path

  df_arr = []
  for csvzip in Path("/content/drive/MyDrive/market_data/").glob("*.zip"):
    print (csvzip)
    for pair in pairs:
      if pair in csvzip.name and resolution in csvzip.name:
        df = pd.read_csv(csvzip)
        df["Instrument"] = csvzip.name[:6]
        df["Returns"] = df["Close"] - df["Close"].shift()
        df["Dttm"] = pd.to_datetime(df["Local time"])
        # bigquery doesn't like the name
        del df["Local time"]
        df_arr.append(df)

  sec_all = pd.concat(df_arr)
  return sec_all

if __name__ == "__main__":
  HOURLY_CHARTS = True
  # "SPY", "DIA", "GBR"
  sec_all = get_drive_csv(pairs=["GBPJPY", "EURUSD"])
  print (sec_all)
  sec = sec_all.query("Instrument=='GBPJPY'")
  sec.columns = sec.columns.str.lower()
  print (sec)

# %%
def get_av_api(symbol, ALPHAVANTAGE_API, function='TIME_SERIES_DAILY_ADJUSTED', params=''):
  """
  If endpoints change or become premium sometimes only option while waiting for
  web.DataReader to update.
  """
  import requests

  # looks like we need to use Daily Adjusted instead of Daily now
  # yahoo could be another potencial replacement

  # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
  symbol = "FXY"

  """
  url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={ALPHAVANTAGE_API}'
  r = requests.get(url)
  data = r.json()
  """

  url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={ALPHAVANTAGE_API}&datatype=csv{params}'
  r = requests.get(url)
  bytes_csv = r.content

  import pandas as pd
  from io import StringIO

  return pd.read_csv(StringIO(bytes_csv.decode("utf8")))  

if __name__ == "__main__":
  # FXY, UUP, GLD
  sec = get_av_api(ALPHAVANTAGE_API, "FXY")
  # sec = get_av_api('BTC', ALPHAVANTAGE_API, 'DIGITAL_CURRENCY_DAILY', params='&market=USD')
  print (sec)
  # dxy = web.DataReader("UUP", "av-daily", api_key=ALPHAVANTAGE_API)

# %%
try:
  import quandl
except:
  # !pip install quandl
  import quandl

def quandl_get_btc():
  import pandas as pd
  # BCHAIN/DIFF
  bchain_price = quandl.get("BCHAIN/MKPRU")
  # bchain_price.plot()

  sec = bchain_price.copy()
  sec["close"] = sec["Value"]
  sec["open"] = sec["high"] = sec["low"] = sec["close"]
  sec["symbol"] = "BTC/USD"
  sec["date"] = pd.to_datetime(sec.index)
  sec["returns"] = sec["close"] - sec["close"].shift()
  del sec["Value"]
  sec_all = sec.copy()
  sec_all = sec_all[sec_all.close!=0.00]
  return sec_all

if __name__ == "__main__":
  sec = quandl_get_btc()
  
