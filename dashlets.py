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
from features_main import get_features_dttm
from features_main import get_features_indicators
from features_main import get_features_cnd
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

  def fetch_symbol_data(universe, symbol, resolution="1D"):
    if resolution == "1D":
      if "/" in symbol:
        sec = web.DataReader(symbol, "av-forex-daily", api_key=universe.api_keys["ALPHAVANTAGE_API"])
      else:
        sec = web.DataReader(symbol, "av-daily-adjusted", api_key=universe.api_keys["ALPHAVANTAGE_API"])
    elif resolution == "1H":
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
    return sec

  def custom_features(self, sec):
    pass

  def add_features(self, sec):
    get_features_dttm(sec)
    get_features_indicators(sec)
    get_features_cnd(sec)
    self.custom_features(sec)

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

  # def aggregate_heatmap(universe, xkey, color_field, ykey, agg_fn, split_by, zkey):
  def aggregate_heatmap(universe, xkey, ykey, zkey, agg_fn):
    data_plt = pd.pivot_table(
        universe.sec_filtered,
        index=xkey,
        values=ykey,
        columns=zkey,
        aggfunc=[
            "mean",
            "std",
            "count",
            winrate
        ])
    return data_plt

  def aggregate(universe, xkey, color_field, ykey, agg_fn, split_by):
    return_field = ykey
    # grp = universe.sec_all.groupby
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

# %%
# add_data
def interactable_plot(
    universe,
    symbols,
    xkey,
    ykey,
    chart_type,
    filter_by,
    agg_fn="mean",
    cum_fn=None,
    color_field="all",
    split_by="symbol",
    # range_slider=(0,1)
    range_start=None,
    # range_length=None,
    future_horizon=None,
    xbin_steps=0,
    ybin_steps=0,
    zbin_steps=0,
    zkey=None,
    # ykeys?
    resolution="1D"
):

  return_field = ykey
  agg_field = agg_fn
  filter_query = filter_by

  hover_col_list = ["count", "std", "winrate", "mean_str", "index"]

  universe.fetch_symbols_data(symbols, resolution)
  universe.remove_orphans(symbols)
  universe.concat_securities()

  # update backtest features
  if future_horizon:
    universe.update_backtest_features(universe.sec_all, future_horizon)

  if xbin_steps > 0:
    universe.create_bin_field(xkey, xbin_steps, na_val=0)
    xkey = xkey + "_bin"
  if ybin_steps > 0:
    universe.create_bin_field(ykey, ybin_steps, na_val=0)
    ykey = ykey + "_bin"
    return_field = ykey
  if zbin_steps > 0:
    universe.create_bin_field(zkey, zbin_steps, na_val=0)
    zkey = zkey + "_bin"
    return_field = zkey

  # filter
  universe.sec_all["all"] = True
  universe.sec_filtered = universe.sec_all.query(filter_by)

  if zkey:
    # universe.df_agg = universe.aggregate_heatmap(xkey, color_field, return_field, agg_fn, split_by, zkey=zkey)
    # return universe.df_agg
    universe.df_agg = universe.aggregate_heatmap(xkey, ykey, zkey, agg_fn)
    return universe.df_agg
    # return universe.df_agg[agg_fn]
    # return universe.df_agg[agg_fn].style.background_gradient(cmap ='magma')
  # ---
  else:
    universe.df_agg = universe.aggregate(xkey, color_field, return_field, agg_fn, split_by)

  if cum_fn:
    agg_field = f"{agg_field}_{cum_fn}"
  kwargs = {
      "kind": chart_type,
      "x": xkey,
      "y": agg_field,
      "by": split_by,
      "hover_cols": hover_col_list
      }
  if zkey:
    kwargs["z"] = zkey
    
  if color_field != "all":
    kwargs["color"] = "color"
  if chart_type == "errorbars":
    kwargs["yerr1"] = "std"
    del kwargs["hover_cols"]

  universe.layout = universe.df_agg.hvplot(**kwargs)

  # re-order x axis: https://discourse.holoviz.org/t/how-to-control-bar-order-in-using-holoviews-bars/51/2
  kwargs = {}
  kwargs[xkey] = list(universe.df_agg[xkey])
  universe.layout = universe.layout.redim.values(**kwargs)
  # final_plt = final_plt.opts(tools=["hover", "tap"])

  # range
  if range_start:
    range_vline = hv.VLine(range_start) * hv.VLine(range_start+future_horizon)
    universe.layout = universe.layout * range_vline

  return universe.layout
