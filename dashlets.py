# %%
try:
  import hvplot.pandas
except:
  !pip install hvplot
  import hvplot.pandas

import holoviews as hv
import panel as pn
from panel import interact
import pandas_datareader.data as web
import pandas as pd
pn.extension()
hv.extension("bokeh")

def add_data(symbols, drill_down, chart_type):
  global sec, sec_dict, sec_all
  for symbol in symbols:
    if not symbol in sec_dict.keys():
      print (f"Loading {symbol}")
      sec = web.DataReader(symbol, "av-forex-daily", api_key=ALPHAVANTAGE_API)
      sec["symbol"] = symbol
      sec["change"] = sec["close"].pct_change()
      sec_dict[symbol] = sec

  sec_arr = sec_dict.values()
  sec_all = pd.concat(sec_arr)

  PREFIX = "ft_" # ts, plt, ft
  sec_all["date"] = pd.to_datetime(sec_all.index)
  # just update all with no elif?
  if drill_down == "YM":
    key = f"ts_dt_YM"
    sec_all[key] = sec_all["date"].dt.strftime("%Y:%m")
  elif drill_down == "doy":
    # sec_all[f"ts_dt_doy"] = sec_all["date"].dt.dayofyear
    sec_all[f"ts_dt_doy"] = sec_all["date"].dt.strftime("%m:%b:%d")
  elif drill_down == "dom":
    sec_all["ts_dt_dom"] = sec_all["date"].dt.day
  elif drill_down == "dow":
    sec_all.loc[:, f"ts_dt_dow"] = sec_all["date"].dt.dayofweek
    sec_all[f"ts_dt_dow"] = sec_all["date"].dt.strftime("%w:%a")

  data_plt = sec_all.groupby(["symbol", f"ts_dt_{drill_down}"])["change"].mean()
  data_plt = data_plt.reset_index()
  data_plt["cumsum"] = data_plt.change.cumsum()

  if chart_type == "bar":
    return data_plt.hvplot.bar(x=f"ts_dt_{drill_down}", y="cumsum", by="symbol")
  else:
    return data_plt.hvplot.line(x=f"ts_dt_{drill_down}", y="cumsum", by="symbol")

def instrument_selector():
  currency_pairs = ["EUR/USD", "AUD/USD", "GBP/USD", "GBP/JPY"]

  dashlet_select = interact(
      add_data,
      symbols=pn.widgets.MultiChoice(options=currency_pairs, value=["EUR/USD"]),
      drill_down=pn.widgets.Select(options=["YM", "doy", "dom", "dow"]),
      chart_type=["line", "bar"]
      )
  return dashlet_select

if __name__ == "__main__":
  display(instrument_selector())