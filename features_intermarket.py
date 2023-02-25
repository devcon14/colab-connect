# from Multimarket optimise
# TODO add combo type metric code

# %%
if __name__ == "__main__":
  sec_all["ft_ta_sma"] = sec_all["close"].rolling(30).mean()
  sec_all["ft_ta_dspi"] = (sec_all["close"] - sec_all["ft_ta_sma"]) / sec_all["ft_ta_sma"] * 100.0
  # sec_all["future_change"] = sec_all["close"].pct_change().shift(-1)

  sec_all

if False:
  import pandas_ta as ta

  # series works
  sec_all["ft_ta_rsi_p14"] = sec_all.groupby("symbol")["close"].apply(lambda x: ta.rsi(close=x, length=14))
  # frame doesn't? returns nan
  sec_all["ft_ta_stochk_p14"] = sec_all.groupby("symbol").apply(lambda sec: ta.stoch(close=sec.close, high=sec.high, low=sec.low, length=14)["STOCHk_14_3_3"])
  sec_all["ft_ta_sma_p100"] = sec_all.groupby("symbol").apply(lambda sec: ta.sma(close=sec.close, length=100))
  sec_all

if False:
  # TODO difference from mean disparity or base/quote mean disparity
  # difference from top rank and bottom rank strength
  DATEFIELD = "datetime" # date

  sec_all["ft_ta_strength"] = sec_all["ft_ta_dspi"]
  sec_all[f"ft_im_strength_rank"] = sec_all.groupby(DATEFIELD)[f"ft_ta_strength"].rank()

  sec_all.sort_values(DATEFIELD)

# pn.extension()
hv.extension("bokeh")

def merge_and_compare(universe, symbol_to_compare):
  # easier and faster way to compare
  df_mg = sec_all.copy()

  # TODO UI to pick symbol
  # FIXME should change to sec_mg
  SYMBOL_STRATEGY = True
  ## symbol_to_compare = "WOOD"
  if SYMBOL_STRATEGY:
    df_mg = df_mg.merge(df_mg.query(f"symbol=='{symbol_to_compare}'")[["date", "ft_ta_dspi"]], on="date", suffixes=("", f"_{symbol_to_compare.replace('/','')}"))

if __name__ == "__main__":
  # asset_count = 3
  TOP_BOTTOM_STRATEGY = True
  if TOP_BOTTOM_STRATEGY:
    asset_count = len(sec_all.symbol.unique())
    df_mg = df_mg.merge(sec_all.query(f"ft_im_strength_rank=={asset_count}")[["date", "ft_ta_strength"]], on="date", suffixes=("", "_top"))
    df_mg = df_mg.merge(sec_all.query("ft_im_strength_rank==1")[["date", "ft_ta_strength"]], on="date", suffixes=("", "_bottom"))

  universe.df_mg = df_mg
  universe.symbol_to_compare = symbol_to_compare

  display(
    interact(merge_and_compare, universe=universe, symbol_to_compare=["WOOD", "SPY", "UUP", "GLD"])
  )
  
