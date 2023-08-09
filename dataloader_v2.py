from datetime import datetime, timedelta
import requests
import pandas as pd
from pathlib import Path

from datetime import datetime, timedelta
import requests
import pandas as pd
from pathlib import Path

class DataLoader:
  """
  >>> DataLoader.configure(ALPHAVANTAGE_API, cache_home='/content/cache')
  >>> slices = DataLoader.get_slices_range(1)

  >>> DataLoader.merge_slices(slices, source="binance", symbol="BTCUSDT", interval="60min")
  >>> DataLoader.merge_slices(["2023-06"], source="yf", symbol="ES=F", interval="60min")

  >>> DataLoader.merge_slices([DataLoader.SLICE_ALL], source="av", symbol="SPY", interval="1d")
  >>> DataLoader.merge_slices([DataLoader.SLICE_ALL], source="yf", symbol="SPY", interval="1d")

  >>> DataLoader.merge_slices(slices, source="yf", symbol="BTC-USD", interval="60min")
  >>> DataLoader.merge_slices(["2023-06"], source="binance", symbol="BTCUSDT", interval="1d")
  """
  ALPHAVANTAGE_API = None
  cache_home = None
  SLICE_ALL = None

  # --- enums
  # only resolutions available on alphavantage
  E_RES_AV_INTRADAY = ["1min", "5min", "15min", "30min", "60min"]
  E_YF_SYMBOLS = ["NQ=F", "ES=F", "GC=F", "SI=F", "^TNX", "DX=F", "^FTSE", "^N225", "^DJI", "^IXIC", "^GSPC", "^GDAXI", "^FCHI", "^HSI", "NG=F", "CL=F", "ZW=F", "ZC=F", "LBR=F"]
  E_AV_SYMBOLS = ["SPY", "GLD", "UUP"]

  def configure(cache_home='/content/cache', api_keys=None):
    """
    >>> configure()
    >>> configure(cache_home='/content/cache', api_keys={"ALPHAVANTAGE_API": "xxx"})
    """
    import json
    if not api_keys:
      with open("/content/drive/MyDrive/market_data/api_keys.json") as fh:
        DataLoader.api_keys = json.load(fh)
    else:
        DataLoader.api_keys = api_keys

    DataLoader.ALPHAVANTAGE_API = DataLoader.api_keys["ALPHAVANTAGE_API"]
    DataLoader.cache_home = cache_home
    DataLoader.create_cache()

  def create_cache():
    p = Path(DataLoader.cache_home)
    if not p.exists():
      p.mkdir()

  def get_slices_range(x):
    # https://stackoverflow.com/questions/6576187/get-year-month-for-the-last-x-months
    import time
    now = time.localtime()
    times = [time.localtime(time.mktime((now.tm_year, now.tm_mon - n, 1, 0, 0, 0, 0, 0, 0))) for n in range(x)]
    slices = [f"{t[0]}-{t[1]:02d}" for t in times]
    return slices

  def read_slice_binance(symbol, interval, _slice=None):
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
    df = pd.read_csv(url)
    df["datetime_local"] = pd.to_datetime(df["open_time"], unit="ms").dt.tz_localize('UTC').dt.tz_convert("Africa/Johannesburg")
    return df

  def read_slice_av(symbol, interval, _slice=None):
    """
    >>> DataLoader.read_slice_av("SPY", _slice=DataLoader.SLICE_ALL, interval="1d")
    >>> DataLoader.read_slice_av("SPY", _slice="2023-06", interval="60min")
    """
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

    df["datetime_local"] = pd.to_datetime(df["timestamp"]).dt.tz_localize('US/Eastern').dt.tz_convert("Africa/Johannesburg")
    return df

  def read_slice_yf(symbol, interval, _slice=None):
    """
    >>> DataLoader.read_slice_yf("^GSPC", _slice=DataLoader.SLICE_ALL, interval="1d")
    >>> DataLoader.read_slice_yf("ES=F", _slice="2023-06", interval="60min")
    """
    import yfinance as yf
    import pandas as pd
    from pandas.tseries.offsets import MonthEnd

    DataLoader.YF_INTERVAL_MAP = {
        "60min": "60m"
    }
    if interval in DataLoader.YF_INTERVAL_MAP:
      interval = DataLoader.YF_INTERVAL_MAP[interval]

    ticker = yf.Ticker(symbol)
    if _slice==DataLoader.SLICE_ALL:
      sec = ticker.history(start="1980-01-01", interval=interval)
    else:
      start_date = f"{_slice}-01"
      end_date = pd.Timestamp(start_date) + MonthEnd(0)
      # end_date += pd.Timedelta(days=1)
      # end_date = end_date.date()
      print (start_date, end_date)
      sec = ticker.history(start=start_date, end=end_date, interval=interval)

    sec["datetime_local"] = pd.to_datetime(sec.index).tz_convert("Africa/Johannesburg")
    # FIXME not sure why it's necessary to convert to datetime again?
    sec["datetime_local"] = pd.to_datetime(sec["datetime_local"])
    sec.columns = [x.lower() for x in sec.columns]
    return sec

  def merge_slices(slices, source="av", symbol="SPY", interval="60min"):
    """
    >>> merge_slices(["2023-06"], source="av|binance", symbol=symbol, interval="1d|60min")

    Returns dataframe of single security.
    """
    slice_arr = []
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
          df = DataLoader.read_slice_av(symbol, interval, _slice)
        elif "yf" in source:
          df = DataLoader.read_slice_yf(symbol, interval, _slice)
        elif source == "binance":
          df = DataLoader.read_slice_binance(symbol, interval, _slice)

      if not Path(cache_fullpath).exists():
        df.to_csv(f'{cache_home}/{cache_filename}.zip', compression={'method': 'zip', 'archive_name': cache_filename}, index=False)

      slice_arr.append(df)

    DataLoader.slice_arr = slice_arr
    sec = pd.concat(slice_arr)

    sec["symbol"] = symbol
    # if we read from cache it's important to convert to datetime again
    sec["datetime_local"] = pd.to_datetime(sec["datetime_local"])
    sec = sec.sort_values(["datetime_local"])
    sec = sec.drop_duplicates(["datetime_local"])
    sec = sec.reset_index(drop=True)
    return sec

  def merge_im_slices(symbols, _slices, interval, source="yf"):
    df_arr = []
    for symbol in symbols:
      df = DataLoader.merge_slices(_slices, source=source, symbol=symbol, interval=interval)
      df_arr.append(df)
    DataLoader.im_arr = df_arr
    return pd.concat(df_arr)
