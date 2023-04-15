"""
From Asymmetric ML
"""

# %%
# global module variables
ALPHAVANTAGE_API = None
sec_all = None
sec = None

# %%
try:
  import hvplot.pandas
except:
  # !pip install hvplot
  import hvplot.pandas

import json
import math
import holoviews as hv
import panel as pn
from panel import interact
import pandas_datareader.data as web
import pandas as pd
# from features_main import get_features_viz as get_viz_features
from features_main import get_features_dttm

# %%
try:
  import hvplot.pandas
except:
  # !pip install hvplot
  import hvplot.pandas

import holoviews as hv
import panel as pn
from panel import interact
import pandas_datareader.data as web
import pandas as pd
# from features_main import get_features_viz as get_viz_features
from features_ta import get_features_ta
from features_main import get_features_dttm
from features_main import get_features_indicators
from features_main import get_features_cnd

from features_ta import get_features_combo
import math


def winrate(x): return (x >= 0).sum() / x.count() * 100.0

class Universe:
  sec = None
  sec_arr = []
  sec_dict = {}
  sec_all = None
  sec_filtered = None
  # sec_mg = None
  api_keys = None
  # ALPHAVANTAGE_API = ALPHAVANTAGE_API

  def __init__(self):
    with open("/content/drive/MyDrive/market_data/api_keys.json") as fh:
      self.api_keys = json.load(fh)

  def fetch_symbol_data(universe, symbol, resolution="1D", source="av"):
    if resolution == "1D":
      if "/" in symbol:
        sec = web.DataReader(symbol, "av-forex-daily", api_key=universe.api_keys["ALPHAVANTAGE_API"])
      else:
        sec = web.DataReader(symbol, "av-daily-adjusted", api_key=universe.api_keys["ALPHAVANTAGE_API"])
    elif resolution == "1H":
      if source == "dukascopy":
        # read from stored dukascopy
        # /content/drive/MyDrive/market_data/AUDCAD_Candlestick_1_Hour_BID_03.01.2006-26.09.2022.zip
        from pathlib import Path
        for p in Path("/content/drive/MyDrive/market_data/").glob(f"{symbol}*"):
          print (p)
          sec = pd.read_csv(p)
          # looks like a bug where it takes GMT+2 and changes it to -2
          # sec["datetime"] = pd.to_datetime(sec["Local time"])

          # so I chop the zone and localize myself
          sec["datetime"] = pd.to_datetime(sec["Local time"].apply(lambda x: x[:24]))
          sec["date"] = sec["datetime"].dt.date
          sec["datetime"] = sec["datetime"].dt.tz_localize('Africa/Johannesburg')

          # probably all I need, dttm functions use index and overwrite later
          sec.set_index("Local time", inplace=True)
          # del sec["Local time"]
          sec.columns = sec.columns.str.lower()
    elif source == "av":
      import requests
      ALPHAVANTAGE_API = universe.api_keys["ALPHAVANTAGE_API"]
      if "/" in symbol:
        pass
      else:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval={resolution}&apikey={ALPHAVANTAGE_API}'
        url += '&outputsize=full'
        sec = pd.read_csv(url)
      # sec.iloc[0].values
    else:
      print ("unexpected parameters")        
    
    sec["symbol"] = symbol
    return sec

  def use_dataframe(universe, sec):
    universe.sec_all = sec
    universe.sec_filtered = sec

  def custom_features(self, sec):
    pass

  def add_features(self, sec):
    """
    Add features for an individual security only before concatenating all securities.
    """
    get_features_dttm(sec)
    get_features_indicators(sec)
    get_features_cnd(sec)
    print ("call custom features")
    self.custom_features(sec)

  def add_features_universe(self, sec_all, custom_params={}):
    """
    Add features to the dataframe of all securities.
    """
    pass

  def update_backtest_features(self, sec_all, future_horizon):
    sec_all["future_returns"] = sec_all["close"].shift(-future_horizon) - sec_all["close"]

  def create_bin_field(universe, field_name, step_multiplier, na_val=0):
    try:
      if na_val is not None:
        universe.sec_all[f"{field_name}_bin"] = universe.sec_all[field_name].fillna(na_val)
      else:
        universe.sec_all[f"{field_name}_bin"] = universe.sec_all[field_name]
      universe.sec_all[f"{field_name}_bin"] = universe.sec_all[f"{field_name}_bin"].apply(lambda x: int(x / step_multiplier) * step_multiplier)
      universe.sec_all["bin_target"] = universe.sec_all[f"{field_name}_bin"]
    except:
      print (f"probably non numeric bin field {field_name}")
    return universe.sec_all[f"{field_name}_bin"].iloc[-5:]

  def fetch_symbols_data(universe, symbols, resolution="1D"):
    for symbol in symbols:
      if not symbol in universe.sec_dict.keys():
        print (f"Loading {symbol}")
        sec = universe.fetch_symbol_data(symbol, resolution)
        sec["symbol"] = symbol
        sec = sec.sort_index()
        universe.add_features(sec)
        universe.sec_dict[symbol] = sec
    # CH01
    universe.remove_orphans(symbols)
    universe.concat_securities()
    universe.add_features_universe(universe.sec_all)

  def concat_securities(universe):
    universe.sec_arr = universe.sec_dict.values()
    universe.sec_all = pd.concat(universe.sec_arr)
    # print (universe.sec_all)
    universe.sec_all = universe.sec_all.sort_values(["date", "symbol"])

  def remove_orphans(universe, symbols):
    # remove orphans
    orphans = []
    for symbol in universe.sec_dict.keys():
      if symbol not in symbols:
        orphans.append(symbol)
    for orphan in orphans:
      del universe.sec_dict[orphan]

  def aggregate(universe, xkey, return_field, agg_fn="mean", split_by="symbol", color_field="all"):
    grp = universe.sec_filtered.groupby([split_by, xkey])[return_field].agg(
        [
            "mean",
            "std",
            "count",
            winrate
        ]
        )
    data_plt = grp.reset_index()
    data_plt[f"{agg_fn}_cumprod"] = data_plt[agg_fn].add(1).cumprod().sub(1)    
    data_plt[f"{agg_fn}_cumsum"] = data_plt[agg_fn].cumsum()
    data_plt[f"{agg_fn}_str"] = data_plt[agg_fn].apply(lambda x: f"{x:.6f}")

    # difficult to make sense of in many cases because everything is aggregated, unless we drop to daily chart
    # now we must aggregate a different column
    print (color_field)
    if color_field != "all":
      # data_plt["color"] = universe.sec_all.groupby([split_by, xkey])[color_field].agg([agg_fn]).reset_index()[agg_fn]
      colors = universe.sec_all.groupby([split_by, xkey])[color_field].agg([agg_fn])
      # make indices align by converting to plain series
      colors = colors.reset_index()[agg_fn]
      data_plt["color"] = colors

    # print (data_plt)
    data_plt = data_plt.sort_values([xkey, split_by])

    return data_plt

# from dashlets import Universe, interactable_plot

class CustomUniverse(Universe):

  def custom_features(self, sec):
    # get_features_ta_windows(sec, [3,5,8])
    # sec["ft_ta_rsi_p2_bin"] = sec["ft_ta_rsi_p2"].apply(lambda x: math.floor(x / 10) * 10 if pd.notnull(x) else 50)

    from finta import TA
    df = TA.WILLIAMS_FRACTAL(sec, period=2)
    # print (df)
    sec["viz_bearish_fractal"] = df["BearishFractal"]
    sec["viz_bullish_fractal"] = df["BullishFractal"]

    sec["ft_ta_kj_trend_up"] = sec["close"] > sec["close"].shift(10)
    """
    for period in [20,50]:
      sec[f"ft_ta_kj_trend_up_p{period}"] = sec["close"] > sec["close"].shift(period)
      # TODO pct_change from open to close (follow), high to close (breakout stops) etc...
    """

    print ("get custom rsi")
    get_features_combo(sec, ["ft_ta_rsi"], [14, 21])
    get_features_combo(sec, ["ft_ta_kj_trend_up"], [20, 50])

    # remember to modify sec not sec_all because df_arr is concatenated later
    # TODO hilo_extension, fib extensions from range
    # close in relation to 10 day high from 10 days ago
    # get_features_combo(universe.sec_all, ["ft_ta_high"], [10])
    # universe.sec_all["ft_lvl_high_ext"] = universe.sec_all["close"] - universe.sec_all["ft_ta_high"].shift(10)

    # TODO show heatmap of indicator combinations within range
    # universe.sec_all["signal_long_timerange"] = (universe.sec_all["ts_dt_"] >= ...)

    sec["signal_long"] = (sec["ft_ta_rsi_p2"] >= 80) & (sec["ft_ta_kj_trend_up_p20"]==False)
    # custom fn
    # sec["signal_long"] = (universe.sec_all["ft_ta_rsi_p2"] >= 80) & (universe.sec_all["ft_ta_kj_trend_up_p20"]==False)
