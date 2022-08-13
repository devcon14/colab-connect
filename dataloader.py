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
def av_forex(pairs):
  import pandas_datareader.data as web
  import pandas as pd
  from time import sleep

  # consider candle_date to avoid sql keywords
  sec_arr = []
  # for pair in ["EUR/USD", "EUR/CHF", "EUR/GBP", "EUR/CAD"]:
  # for pair in ["USD/EUR", "CHF/EUR", "GBP/EUR", "CAD/EUR"]:
  # for pair in ["AUD/EUR", "NZD/EUR"]:
  # for pair in ["GBP/USD", "JPY/USD", "CHF/USD"]:
  # for pair in ["GBP/USD"]:
  # for pair in ["CAD/USD", "NZD/USD", "AUD/USD"]:
  # for pair in ["USD/JPY", "EUR/JPY", "CAD/JPY"]:
  # for pair in ["AUD/JPY", "NZD/JPY", "GBP/JPY"]:
  for pair in pairs:
    print (pair)
    sec = web.DataReader(pair, "av-forex-daily", api_key=ALPHAVANTAGE_API)
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
  
