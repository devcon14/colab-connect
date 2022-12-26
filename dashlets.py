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

import holoviews as hv
import panel as pn
from panel import interact
import pandas_datareader.data as web
import pandas as pd
from features_main import get_features_viz as get_viz_features

def add_data(sec_inst, symbols, drill_down, chart_type):
  global sec, sec_dict, sec_all
  for symbol in symbols:
    if not symbol in sec_inst.sec_dict.keys():
      print (f"Loading {symbol}")
      if "/" in symbol:
        sec = web.DataReader(symbol, "av-forex-daily", api_key=sec_inst.ALPHAVANTAGE_API)
      else:
        sec = web.DataReader(symbol, "av-daily-adjusted", api_key=sec_inst.ALPHAVANTAGE_API)
      sec["symbol"] = symbol
      sec = sec.sort_index()
      get_viz_features(sec)
      # sec = sec.sort_values("date")
      sec_inst.sec_dict[symbol] = sec

  sec_inst.sec_arr = sec_inst.sec_dict.values()
  sec_inst.sec_all = pd.concat(sec_inst.sec_arr)

  PREFIX = "ft_" # ts, plt, ft
  data_plt = sec_inst.sec_all.groupby(["symbol", f"ts_dt_{drill_down}"])["change"].mean()
  # data_plt = data_plt.sort_values(f"ts_dt_{drill_down}")
  data_plt = data_plt.reset_index()
  data_plt["cumsum"] = data_plt.change.cumsum()

  if chart_type == "bar":
    return data_plt.hvplot.bar(x=f"ts_dt_{drill_down}", y="cumsum", by="symbol")
  else:
    return data_plt.hvplot.line(x=f"ts_dt_{drill_down}", y="cumsum", by="symbol")

def instrument_selector(sec_inst, currency_pairs = ["EUR/USD", "GBP/USD", "CHF/USD", "JPY/USD", "AUD/USD", "CAD/USD"], value=["EUR/USD"]):

  dashlet_select = interact(
      add_data,
      sec_inst=sec_inst,
      symbols=pn.widgets.MultiChoice(options=currency_pairs, value=["EUR/USD"]),
      drill_down=pn.widgets.Select(options=["YM", "decennial", "shmita", "season_13", "election", "doy", "dom", "dow"]),
      chart_type=["line", "bar"]
      )
  return dashlet_select

class Universe:
  sec = None
  sec_arr = []
  sec_dict = {}
  sec_all = None
  ALPHAVANTAGE_API = ALPHAVANTAGE_API

if __name__ == "__main__":
  pn.extension(comms="colab")
  # hv.extension("bokeh")

  sec_inst = Universe()

  majors = ["EUR/USD", "GBP/USD", "CHF/USD", "JPY/USD", "AUD/USD", "CAD/USD"]
  etfs = ["SPY", "GLD", "UUP", "SLV"]
  # start_vals = ["EUR/USD", "GBP/USD", "CHF/USD"]
  start_vals = ["GLD"]
  display(instrument_selector(sec_inst, etfs, start_vals))

# %%
def optimum_1d(symbol, filter_query, x_feature, return_field = "returns"):

  # --- symbol select, prep features
  sec = sec_all.query(f"symbol=='{symbol}'").copy()

  sec["pct_change"] = sec["close"].pct_change()
  # sec["future_returns"] = sec["close"].shift(5) - sec["close"]
  # force 1 day returns
  if "returns" not in sec.columns:
    sec["returns"] = sec["close"].shift() - sec["close"]
  sec["all"] = True

  # binning
  # sec["ft_im_bottom_div_bin"] = (sec["ft_im_bottom_div"] * 10.0).astype(int)
  # sec[f"ft_im_{compare_symbol}_dspidiv_bin"] = (sec[f"ft_im_{compare_symbol}_dspidiv"] * 10.0).astype(int)
  # ---

  sec_flt = sec.query(filter_query)
  if x_feature:
    # data_returns = sec_flt.query(filter_query).groupby(x_feature)["ddratio"].mean()
    data_returns = sec_flt.query(filter_query).groupby(x_feature)[return_field].mean()
    # data_returns = sec_flt.groupby(x_feature).bt_pct_change.mean()
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
    data_returns = sec_flt.bt_pct_change.mean()
    data_count = sec_flt.future_returns.count()
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
