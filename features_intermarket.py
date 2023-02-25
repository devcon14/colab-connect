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
