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

def add_data(universe, symbols, drill_down, chart_type):

  for symbol in symbols:
    if not symbol in universe.sec_dict.keys():
      print (f"Loading {symbol}")
      if hasattr(universe, "reader_fn"):
        sec = universe.reader_fn(symbol)
      else:
        if "/" in symbol:
          sec = web.DataReader(symbol, "av-forex-daily", api_key=universe.ALPHAVANTAGE_API)
        else:
          sec = web.DataReader(symbol, "av-daily-adjusted", api_key=universe.ALPHAVANTAGE_API)
      sec["symbol"] = symbol
      sec = sec.sort_index()
      get_features_dttm(sec)
      # sec = sec.sort_values("date")
      universe.sec_dict[symbol] = sec

  # remove orphans
  orphans = []
  for symbol in universe.sec_dict.keys():
    if symbol not in symbols:
      orphans.append(symbol)
  for orphan in orphans:
    del universe.sec_dict[orphan]

  universe.sec_arr = universe.sec_dict.values()
  universe.sec_all = pd.concat(universe.sec_arr)
  universe.sec_all = universe.sec_all.sort_values(["date", "symbol"])

  if "ft" in drill_down:
    xkey = drill_down
  else:
    xkey = f"ts_dt_{drill_down}"

  PREFIX = "ft_" # ts, plt, ft
  grp = universe.sec_all.groupby(["symbol", xkey])["change"].agg(["mean", "std"])
  data_plt = grp.reset_index()
  data_plt["cumsum"] = data_plt["mean"].cumsum()

  if chart_type == "bar":
    return data_plt.hvplot.bar(x=xkey, y="cumsum", by="symbol")
  elif chart_type == "error":
    line_plt = data_plt.hvplot.line(x=xkey, y="cumsum", by="symbol")
    error_plt = data_plt.hvplot.errorbars(x=xkey, y="cumsum", yerr1="std", by="symbol")
    return line_plt * error_plt
  elif chart_type == "line":
    line_plt = data_plt.hvplot.line(x=xkey, y="cumsum", by="symbol")
    return line_plt


def instrument_selector(universe, currency_pairs, value, chart_type):

  dashlet_select = interact(
      add_data,
      universe=universe,
      symbols=pn.widgets.MultiChoice(options=currency_pairs, value=value),
      drill_down=pn.widgets.Select(options=["YM", "decennial", "shmita", "season_9", "season_13", "election", "doy", "ft_dt_bizday-n", "ft_dt_bizday+n", "dom", "dow"]),
      chart_type=chart_type
      )
  return dashlet_select

class Universe_:
  sec = None
  sec_arr = []
  sec_dict = {}
  sec_all = None
  # sec_mg = None
  ALPHAVANTAGE_API = ALPHAVANTAGE_API

if __name__ == "__main__":
  pn.extension(comms="colab")
  # hv.extension("bokeh")

  universe = Universe()
  universe.ALPHAVANTAGE_API = ALPHAVANTAGE_API
  # universe.reader_fn = av_reader

  majors = ["EUR/USD", "GBP/USD", "CHF/USD", "JPY/USD", "AUD/USD", "CAD/USD"]
  etfs = ["SPY", "GLD", "UUP", "SLV"]
  # start_vals = ["EUR/USD", "GBP/USD", "CHF/USD"]
  start_vals = ["GLD"]
  dashlet_instrument = instrument_selector(universe, etfs, start_vals, ["line", "bar", "error"])
  display(dashlet_instrument)

# %%
def optimum_1d(symbol, filter_query, x_feature, return_field = "returns"):

  # --- symbol select, prep features
  sec = sec_all.query(f"symbol=='{symbol}'").copy()

  # sec["future_returns"] = sec["close"].shift(5) - sec["close"]
  # force 1 day returns
  if "returns" not in sec.columns:
    sec["returns"] = sec["close"].shift() - sec["close"]
  if "change" not in sec.columns:
    sec["change"] = sec["close"].pct_change()
  sec["all"] = True

  # binning
  # sec["ft_im_bottom_div_bin"] = (sec["ft_im_bottom_div"] * 10.0).astype(int)
  # sec[f"ft_im_{compare_symbol}_dspidiv_bin"] = (sec[f"ft_im_{compare_symbol}_dspidiv"] * 10.0).astype(int)
  # ---

  sec_flt = sec.query(filter_query)
  if x_feature:
    data_returns = sec_flt.query(filter_query).groupby(x_feature)[return_field].mean()
    data_count = sec_flt.groupby(x_feature)[return_field].count()
    
    # avoid divide by 0 in some situations
    GET_WINRATE = False
    if GET_WINRATE:
      # win count
      data_winrate = sec_flt.query(filter_query).groupby(x_feature).apply(lambda x: sum(x[return_field] >= 0) / x[return_field].count() * 100.0)
      mg = pd.concat([data_returns, data_count, data_winrate], axis="columns")
      mg.columns = ["mean", "count", "winrate"]
    else:
      mg = pd.concat([data_returns, data_count], axis="columns")
      mg.columns = ["mean", "count"]
      mg["mean_str"] = mg["mean"].apply(lambda x: f"{x:.6f}")

    # mg["pctcumsum"] = mg["mean"].cumsum()
    # print (mg)
    # data_returns.plot()
    # return data_returns.hvplot.line()
    return mg.hvplot.bar(y="mean", hover_cols=["count", "winrate", "mean_str"])
    # return mg.hvplot.line()
    # return pn.Row(      data_returns.hvplot(),      # data_count.hvplot()  )
  else:
    data_returns = sec_flt[return_field].mean()
    data_count = sec_flt[return_field].count()
    return pn.indicators.Number(name='Mean Returns', value=data_returns, format='{value:.5f}')

# %%
# do we call this a multipler or divisor?
def create_bin_field(field_name, step_multiplier, na_val=0):
  if na_val is not None:
    sec[f"{field_name}_bin"] = sec[field_name].fillna(na_val)
  else:
    sec[f"{field_name}_bin"] = sec[field_name]
  # or floor, ceil, round
  sec[f"{field_name}_bin"] = sec[f"{field_name}_bin"].apply(lambda x: int(x / step_multiplier) * step_multiplier)
  return sec[f"{field_name}_bin"].iloc[-5:]

if __name__ == "__main__":
  sec = df_mg
  # print(df_mg.columns)
  dashlet_binner = interact(
      create_bin_field,
      field_name=["ft_im_bottom_div", 'ft_im_WOOD_dspidiv', "ft_ta_rsi_p2"],
      step_multiplier=[1.0, 2, 5, 10.0, 100.0],
      na_val=[0, None, 50],
      return_field=["returns", "future_returns", "mfe"]
      )
  display(dashlet_binner)

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
        
  def fetch_symbol_data(universe, symbol):
    if "/" in symbol:
      sec = web.DataReader(symbol, "av-forex-daily", api_key=universe.api_keys["ALPHAVANTAGE_API"])
    else:
      sec = web.DataReader(symbol, "av-daily-adjusted", api_key=universe.api_keys["ALPHAVANTAGE_API"])
    return sec

  def add_features(self, sec):
    get_features_dttm(sec)
    get_features_indicators(sec)
    get_features_cnd(sec)
    # get_features_ta_windows(sec, [3,5,8])
    # sec["ft_ta_rsi_p2_bin"] = sec["ft_ta_rsi_p2"].apply(lambda x: math.floor(x / 10) * 10 if pd.notnull(x) else 50)

    from finta import TA
    df = TA.WILLIAMS_FRACTAL(sec, period=2)
    # print (df)
    sec["viz_bearish_fractal"] = df["BearishFractal"]
    sec["viz_bullish_fractal"] = df["BullishFractal"]

    sec["ft_ta_kj_trend_up"] = sec["close"] > sec["close"].shift(10)

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

  def fetch_symbols_data(universe, symbols):
    for symbol in symbols:
      if not symbol in universe.sec_dict.keys():
        print (f"Loading {symbol}")
        sec = universe.fetch_symbol_data(symbol)
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
    if color_field != "all":
      data_plt["color"] = universe.sec_all.groupby([split_by, xkey])[color_field].agg([agg_fn]).reset_index()[agg_fn]

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
    bin_steps=0
):

  return_field = ykey
  agg_field = agg_fn
  filter_query = filter_by

  hover_col_list = ["count", "std", "winrate", "mean_str", "index"]

  universe.fetch_symbols_data(symbols)
  universe.remove_orphans(symbols)
  universe.concat_securities()

  # update backtest features
  if future_horizon:
    universe.update_backtest_features(universe.sec_all, future_horizon)

  if bin_steps > 0:
    universe.create_bin_field(xkey, bin_steps, na_val=0)
    xkey = xkey + "_bin"

  # filter
  universe.sec_all["all"] = True
  universe.sec_filtered = universe.sec_all.query(filter_by)

  data_plt = universe.aggregate(xkey, color_field, return_field, agg_fn, split_by)
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
