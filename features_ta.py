# from features_candlestick.py
def get_features_ta(sec):
  # %%
  # 40in20out
  sec["ft_high_past_40"] = sec["high"].rolling(40).max()
  sec["ft_low_past_40"] = sec["low"].rolling(40).min()
  sec["ft_high_past_20"] = sec["low"].rolling(20).min()
  sec["ft_low_past_20"] = sec["low"].rolling(20).min()
  sec["ft_cross_high_40"] = (sec["ft_high_past_40"] > sec["ft_high_past_40"].shift(1))
  sec["ft_cross_low_40"] = (sec["ft_low_past_40"] > sec["ft_low_past_40"].shift(1))
  sec.ft_cross_high_40.sum()

  # %%
  # volume spike
  if "volume" in sec.columns:
    multiplier = 2.0
    SMA = 20
    sec["ft_volume_spike"] = sec["volume"] > multiplier * sec["volume"].rolling(SMA).mean()
    sec["ft_volume_spike"].sum()

# from features_main
def get_features_indicators(sec, periods=[2]):
  import pandas_ta as ta
  sec["ft_ta_sma_p200"] = sec["close"].rolling(200).mean()
  
  for period in periods:
    # sec["ft_ta_stoch_p14"] = ta.stoch(close=sec.close, high=sec.high, low=sec.low, length=14)["STOCHk_14_3_3"]
    sec[f"ft_ta_rsi_p{period}"] = ta.rsi(close=sec.close, length=period)
    sec[f"ft_ta_kj_trend_up_p{period}"] = sec["close"] > sec["close"].shift(period)

# %%
def get_features_combo(sec, indicators, periods):
  for period in periods:
    for indicator in indicators:
      if indicator == "ft_ta_hilo_spread":
        sec[f"ft_ta_high_p{period}"] = sec["high"].rolling(period).max()
        sec[f"ft_ta_low_p{period}"] = sec["low"].rolling(period).min()
        sec[f"{indicator}_p{period}"] = sec[f"ft_ta_high_p{period}"] - sec[f"ft_ta_low_p{period}"]
      if indicator == "ft_ta_high":
        sec[f"{indicator}_p{period}"] = sec["high"].rolling(period).max()
        # sec["ft_cross_high_40"] = (sec["ft_high_past_40"] > sec["ft_high_past_40"].shift(1))
        sec[f"ft_ta_cross_high_{period}"] = sec["high"] > sec[f"ft_ta_high_p{period}"].shift(1)
      if indicator == "ft_ta_low":
        sec[f"{indicator}_p{period}"] = sec["low"].rolling(period).min()
        sec[f"ft_ta_cross_low_{period}"] = sec["low"] < sec[f"ft_ta_low_p{period}"].shift(1)
      if indicator == "ft_ta_kj_trend_up":
        sec[f"ft_ta_kj_trend_up_p{period}"] = sec["close"] >= sec["close"].shift(period)
      if indicator == "ft_ta_kj_trend_down":
        sec[f"ft_ta_kj_trend_down_p{period}"] = sec["close"] < sec["close"].shift(period)
      if indicator == "ft_ta_sma":
        sec[f"{indicator}_p{period}"] = sec["close"].rolling(period).mean()
      if indicator == "ft_ta_dspi":
        sec[f"ft_ta_sma_p{period}"] = sec["close"].rolling(period).mean()
        sec[f"ft_ta_dspi_p{period}"] = (sec["close"] - sec[f"ft_ta_sma_p{period}"]) / sec[f"ft_ta_sma_p{period}"] * 100.0
      # from kaggle stats bundle
      if indicator == "ft_ta_stats":
        series = sec["close"]
        r = series.rolling(period)
        m = r.mean()
        s = r.std()
        z = (series-m)/s
        dsp = (series-m)/series
        prefix = "ft_ta"
        sec[f'{prefix}_dspi_p{period}'] = dsp
        add_zscore = True
        if add_zscore:
            sec[f'{prefix}_zscore_p{period}'] = z
            sec[f'{prefix}_std_p{period}'] = s

      try:
        import pandas_ta as ta
        if indicator == "ft_ta_rsi":
          sec[f"{indicator}_p{period}"] = ta.rsi(close=sec.close, length=period)
        if indicator == "ft_ta_stoch":
          sec[f"ft_ta_stoch_p{period}"] = ta.stoch(close=sec.close, high=sec.high, low=sec.low, length=14)[f"STOCHk_{period}_3_3"]
      except:
        if indicator in ["ft_ta_rsi", "ft_ta_stoch"]:
          print (f"missing ta libraries for {indicator}")
