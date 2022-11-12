def get_features_cnd(sec):
  # %%
  # range / volatility
  """
  While I'm here:
  https://www.quantifiedstrategies.com/nassim-taleb-strategy/
  https://www.quantifiedstrategies.com/divergence-trading-strategy/
  https://www.quantifiedstrategies.com/the-option-expiration-week-effect/
  https://www.quantifiedstrategies.com/overnight-trading/
  """
  # https://www.quantifiedstrategies.com/nr7-trading-strategy/
  # Profit factor 1.81, can buy a bump to 2.35
  sec["ft_cnd_range"] = sec["high"] - sec["low"]
  for period in [4, 7]:
    sec[f"ft_cnd_nr_{period}"] = sec["ft_cnd_range"] == sec["ft_cnd_range"].rolling(period).min()
    sec[f"ft_cnd_wr_{period}"] = sec["ft_cnd_range"] == sec["ft_cnd_range"].rolling(period).max()
  sec
  
  # %%
  sec["ft_cnd_high_breakout"] = sec["high"] > sec["high"].shift(1)
  sec["ft_cnd_low_breakout"] = sec["low"] < sec["low"].shift(1)
  # kaggle convention was ft_can_lower_shadow...
  # could try auto prefix all columns
  sec["ft_cnd_lower_shadow"] = sec.query("high != low").apply(lambda x: (min(x["open"], x["close"])-x["low"]) / (x["high"] - x["low"]), axis="columns")
  sec["ft_cnd_upper_shadow"] = sec.query("high != low").apply(lambda x: (x["high"]-max(x["open"], x["close"])) / (x["high"] - x["low"]), axis="columns")

  sec["ft_cnd_closing_high"] = sec["close"] > sec["high"].shift(1)
  sec["ft_cnd_closing_low"] = sec["close"] < sec["low"].shift(1)
  sec["ft_cnd_closing_high_1"] = sec["ft_cnd_closing_high"].shift(1)
  sec["ft_cnd_closing_low_1"] = sec["ft_cnd_closing_low"].shift(1)

  # the strat
  sec["tmp_high_1"] = sec["high"].shift(1)
  sec["tmp_low_1"] = sec["low"].shift(1)
  sec["ft_cnd_inside_bar"] = (sec["high"] < sec["tmp_high_1"]) & (sec["low"] > sec["tmp_low_1"])
  # engulfing / outside bar
  sec["ft_cnd_engulfing"] = (sec["high"] > sec["tmp_high_1"]) & (sec["low"] < sec["tmp_low_1"])
  sec["ft_cnd_green_soldier"] = (sec["high"] > sec["tmp_high_1"]) & (sec["low"] > sec["tmp_low_1"])
  sec["ft_cnd_red_soldier"] = (sec["high"] < sec["tmp_high_1"]) & (sec["low"] < sec["tmp_low_1"])
  

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
    
