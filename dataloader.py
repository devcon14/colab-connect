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
def get_econdb_fx():
  # NOTE ticker id is in the series part of the url
  # https://www.econdb.com/series/Y10YDJP/japan-long-term-yield/
  # use CPI for (IS) inflation

  # today's stats with inflation
  # https://econdb.com/country_stats/crosstable/?format=csv
  # Maybe quandl for inflation although YOY updated monthly: RATEINF/INFLATION_USA, RATEINF/INFLATION_JPN
  import pandas as pd

  dataframes = {}
  _stat_items = [
      ("CPI US", "CPIUS"),
      ("Unemployment US", "URATEUS")
  ]
  stat_items = [
      ("CPI Japan", "CPIJP"),
      ("Yield JP", "Y10YDJP"),
      ("Unemployment JP", "URATEJP"),
      ("CPI US", "CPIUS"),
      ("Yield US", "Y10YDUS"),
      ("Unemployment US", "URATEUS")
      ]
  for name, ticker_id in stat_items:
    print (name)
    dataframes[ticker_id] = pd.read_csv(
      f'https://www.econdb.com/api/series/{ticker_id}/?format=csv',
      index_col='Date', parse_dates=['Date'])
  return dataframes

if __name__ == "__main__":
  dataframes = get_econdb_fx()
  import pandas as pd
  from functools import reduce

  # dataframes["CPIUS"]
  df = reduce(lambda x, y: pd.merge(x, y, on="Date"), dataframes.values())
  display(df)

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
  # For BTC: alphavantage daily only till 2019-12-24 ...
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
  try:
    import quandl
  except:
    # !pip install quandl
    import quandl
  sec = quandl_get_btc()

# %%
def quandl_get_cot(currency):
  # quandl COT
  # https://www.tradingview.com/script/B6J550VX-COT-Report-Indicator/
  include_options = "" # "O" or ""
  # largest 4 long, short etc..
  use_concentration = "" # "_CR" or ""
  # legacy goes much further back but not as detailed
  legacy_format = "" # "_L" or ""

  code = "098662" # USD index (ICUS) / ICE?
  code = "116661" # NYCE:EUR/USD
  code = "099741" # CME:EURO

  # CME codes
  # all their instruments seem relative to USD, interest rate futures might be useful
  codes = {
      "EUR": "099741",
      "JPY": "097741",
      "AUD": "232741",
      "CAD": "090741",
      "GBP": "096742",
      "NZD": "112741",
      "CHF": "092741"
  }
  code = codes[currency]

  dataset_code = f"CFTC/{code}_F{include_options}{legacy_format}_ALL{use_concentration}"
  print (dataset_code)
  df = quandl.get(dataset_code)

  df["ft_fnd_cot_leverage_funds_net_long"] = df["Leveraged Funds Longs"] - df["Leveraged Funds Shorts"]
  df["ft_fnd_cot_dealer_net_long"] = df["Dealer Longs"] - df["Dealer Shorts"]
  df["ft_fnd_asset_manager_net_long"] = df["Asset Manager Longs"] - df["Asset Manager Shorts"]

  # shows as Monday or Tuesday
  # df["dt_dow"] = df.index.dayofweek
  # df["dt_dow"] = df.index.strftime("%a")
  return df

if __name__ == "__main__":
  try:
    import quandl
  except:
    # !pip install quandl
    import quandl

  df = quandl_get_cot("EUR")
  display(
      # df[["Leveraged Funds Longs", "Leveraged Funds Shorts", "Open Interest"]]
      df
  )
