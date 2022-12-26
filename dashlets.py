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

def get_viz_features(sec):
    # must apply with 1 symbol
    sec["change"] = sec["close"].pct_change()

    # can apply with all
    sec_all = sec
    sec_all["date"] = pd.to_datetime(sec_all.index)

    # -- YM
    sec_all["ts_dt_YM"] = sec_all["date"].dt.strftime("%Y:%m")
    # -- cycles
    sec_all["ts_dt_decennial"] = pd.Series(sec_all["date"].dt.year % 10).astype(str) + sec_all["date"].dt.strftime(":%m")
    sec_all["ts_dt_shmita"] = pd.Series(sec_all["date"].dt.year % 7).astype(str) + sec_all["date"].dt.strftime(":%m")
    sec_all["ts_dt_election"] = pd.Series(sec_all["date"].dt.year % 4).astype(str) + sec_all["date"].dt.strftime(":%m")
    # -- doy
    # sec_all[f"ts_dt_doy"] = sec_all["date"].dt.dayofyear
    sec_all[f"ts_dt_doy"] = sec_all["date"].dt.strftime("%m:%b:%d")
    # -- dom
    sec_all["ts_dt_dom"] = sec_all["date"].dt.day
    # -- dow
    # sec_all.loc[:, f"ts_dt_dow"] = sec_all["date"].dt.dayofweek
    sec_all[f"ts_dt_dow"] = sec_all["date"].dt.strftime("%w:%a")

def add_data(sec_inst, symbols, drill_down, chart_type):
  global sec, sec_dict, sec_all
  for symbol in symbols:
    if not symbol in sec_inst.sec_dict.keys():
      print (f"Loading {symbol}")
      if "/" in symbol:
        sec = web.DataReader(symbol, "av-forex-daily", api_key=ALPHAVANTAGE_API)
      else:
        sec = web.DataReader(symbol, "av-daily-adjusted", api_key=ALPHAVANTAGE_API)
      sec["symbol"] = symbol
      sec = sec.sort_index()
      get_viz_features(sec)
      # sec = sec.sort_values("date")
      sec_inst.sec_dict[symbol] = sec

  sec_inst.sec_arr = sec_inst.sec_dict.values()
  sec_inst.sec_all = pd.concat(sec_inst.sec_arr)

  PREFIX = "ft_" # ts, plt, ft
  data_plt = sec_inst.sec_all.groupby(["symbol", f"ts_dt_{drill_down}"])["change"].mean()
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
      drill_down=pn.widgets.Select(options=["YM", "decennial", "shmita", "election", "doy", "dom", "dow"]),
      chart_type=["line", "bar"]
      )
  return dashlet_select

class Universe:
  sec = None
  sec_arr = []
  sec_dict = {}
  sec_all = None

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
def optimum_1d(symbol, filter_query, x_feature):

  # --- symbol select, prep features
  sec = sec_all.query(f"symbol=='{symbol}'").copy()
  sec["bt_pct_change"] = sec["close"].pct_change()
  sec["future_returns"] = sec["close"].shift(5) - sec["close"]

  # force 1 day returns
  sec["returns"] = sec["close"].shift() - sec["close"]
  sec["all"] = True

  # binning
  # sec["ft_im_bottom_div_bin"] = (sec["ft_im_bottom_div"] * 10.0).astype(int)
  # sec[f"ft_im_{compare_symbol}_dspidiv_bin"] = (sec[f"ft_im_{compare_symbol}_dspidiv"] * 10.0).astype(int)
  # ---

  return_field = "returns"
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

