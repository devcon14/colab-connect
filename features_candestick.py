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

def get_features_pa(sec):
  # %%
  # 40in20out
  sec["ft_high_past_40"] = sec["high"].rolling(40).max()
  sec["ft_low_past_20"] = sec["low"].rolling(20).min()
  sec["ft_cross_high_40"] = (sec["ft_high_past_40"] > sec["ft_high_past_40"].shift(1))
  sec["ft_cross_low_20"] = (sec["ft_low_past_20"] > sec["ft_low_past_20"].shift(1))
  sec.ft_cross_high_40.sum()

  # %%
  # volume spike
  if "volume" in sec.columns:
    multiplier = 2.0
    SMA = 20
    sec["ft_volume_spike"] = sec["volume"] > multiplier * sec["volume"].rolling(SMA).mean()
    sec["ft_volume_spike"].sum()
    
