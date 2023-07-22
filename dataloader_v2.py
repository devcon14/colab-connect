from datetime import datetime, timedelta
import requests
import pandas as pd
from pathlib import Path

class DataLoader:
  """
  >>> DataLoader.configure(ALPHAVANTAGE_API, cache_home='/content/cache')
  >>> DataLoader.read_slice_av("SPY", _slice="NA", interval="1d")
  >>> slices = DataLoader.get_slices_range(1)
  >>> DataLoader.merge_slices(slices, source="binance", symbol="BTCUSDT", interval="60min")
  >>> DataLoader.merge_slices(["NA"], source="av", symbol="SPY", interval="1d")
  """
  ALPHAVANTAGE_API = None
  cache_home = None

  # --- enums
  # only resolutions available on alphavantage
  E_RES_AV_INTRADAY = ["1min", "5min", "15min", "30min", "60min"]
  COMMON_SYMBOLS = ["SPY", "GLD", "UUP"]

  def configure(ALPHAVANTAGE_API, cache_home='/content/cache'):
    DataLoader.ALPHAVANTAGE_API = ALPHAVANTAGE_API
    DataLoader.cache_home = cache_home
    DataLoader.create_cache()

  def create_cache():
    p = Path(DataLoader.cache_home)
    if not p.exists():
      p.mkdir()

  def get_slices_range(end):
    slices = []
    for index in range(1, end):
      dt = datetime.now() - timedelta(weeks=4*index)
      slices.append(dt.strftime("%Y-%m"))
    return slices

  def read_slice_binance(symbol, _slice, interval):
    binance_resolution_map = {
        "1min": "1m",
        "5min": "5m",
        "30min": "30m",
        "60min": "1h",
        "1d": "1d"
    }
    resolution = binance_resolution_map[interval]
    url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{resolution}/BTCUSDT-{resolution}-{_slice}.zip"
    print (url)
    binance_df = pd.read_csv(url)
    return binance_df

  def read_slice_av(symbol, _slice, interval):
    # url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval={interval}&slice={slice}&apikey={DataLoader.ALPHAVANTAGE_API}'

    # API changes, _EXTENDED merged
    # looks like zone is now eastern
    url = f'https://www.alphavantage.co/query?'
    if "/" in symbol:
      from_symbol, to_symbol = symbol.split("/")
      url += f'&function=FX_DAILY&from_symbol={from_symbol}&to_symbol={to_symbol}'
    elif "min" in interval:
      url += f"symbol={symbol}"
      url += f'&function=TIME_SERIES_INTRADAY&interval={interval}'
    else:
      url += f"symbol={symbol}"
      # which endpoint is premium seems to change once in a while
      # url += '&function=TIME_SERIES_DAILY_ADJUSTED'
      url += '&function=TIME_SERIES_DAILY'

    # url += f"&slice={_slice}"
    url += f"&month={_slice}"
    url += f"&apikey={DataLoader.ALPHAVANTAGE_API}"

    # By default, extended_hours=true and the output time series will include both the regular trading hours and the extended trading hours (4:00am to 8:00pm Eastern Time for the US market). Set extended_hours=false to query regular trading hours (9:30am to 16:00pm US Eastern Time) only.
    # I expect standard hours between 15:30 SAST (09:30) to 22:00 SAST (16:00)
    # extended is probably still less hours than futures
    url += "&extended_hours=true"

    # By default, adjusted=true
    url += "&datatype=csv"

    url += '&outputsize=full'
    import pandas as pd
    print (url)
    df = pd.read_csv(url)
    return df

  def merge_slices(slices, source="av", symbol="SPY", interval="60min"):
    """
    >>> merge_slices(["2023-06"], source="av|binance", symbol=symbol, interval="1d|60min")
    
    Returns dataframe of single security.
    """
    df_arr = []
    for _slice in slices:
      # cache_home = '/content/drive/MyDrive/market_data/cache'
      cache_home = '/content/cache'
      cache_filename = f'{source}_{symbol.replace("/", "")}_{interval}_{_slice}.csv'
      cache_fullpath = f'{cache_home}/{cache_filename}.zip'
      if Path(cache_fullpath).exists():
        print (f"read cache {cache_fullpath}")
        df = pd.read_csv(cache_fullpath)
      else:
        if "av" in source:
          df = DataLoader.read_slice_av(symbol, _slice, interval)
        elif source == "binance":
          df = DataLoader.read_slice_binance(symbol, _slice, interval)

      if "av" in source:
        df["datetime_local"] = pd.to_datetime(df["timestamp"]).dt.tz_localize('US/Eastern').dt.tz_convert("Africa/Johannesburg")
      elif source == "binance":
        df["datetime_local"] = pd.to_datetime(df["open_time"], unit="ms").dt.tz_localize('UTC').dt.tz_convert("Africa/Johannesburg")

      if not Path(cache_fullpath).exists():
        df.to_csv(f'{cache_home}/{cache_filename}.zip', compression={'method': 'zip', 'archive_name': cache_filename}, index=False)

      df_arr.append(df)

    sec = pd.concat(df_arr)
    sec["symbol"] = symbol
    sec = sec.drop_duplicates(["datetime_local"])
    return sec
