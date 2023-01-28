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
  
# from smash days
def backtest_features(df, future_horizon = 5):
  df["future_returns"] = df["close"].shift(-future_horizon) - df["close"]
  # TODO long and short mae,mfe
  df["long_mae"] = df["close"].rolling(future_horizon).apply(lambda x: x[0] - np.min(x))
  df["long_mae"] = df["long_mae"].shift(-future_horizon)
  df["long_mfe"] = df["close"].rolling(future_horizon).apply(lambda x: np.max(x) - x[0])
  df["long_mfe"] = df["long_mfe"].shift(-future_horizon)

  df["bt_pct_change"] = df["close"].pct_change()
  # df["ddratio"] = df["future_returns"] / df["mae"]
  df["bt_long_f2a_ratio"] = df["long_mfe"] / (df["long_mae"]+1)

  # future daily returns
  ## df["daily_returns"] = df["close"].shift(-1) - df["close"]
  ## df["daily_session_returns"] = df["close"] - df["open"]
  # yesterday's close - today's open
  ## df["overnight_returns"] = df["close"].shift(1) - df["open"]

  # fixed broken calculations
  # -----------
  # today's close - yesterday's close
  df["returns"] = df["close"] - df["close"].shift(1)
  df["session_returns"] = df["close"] - df["open"]
  # today's open - yesterday's close
  df["gap_returns"] = df["open"] - df["close"].shift(1)

def set_future_returns(sec, future_horizon=3):
  # set future returns
  sec["future_returns"] = sec["close"].shift(-future_horizon) - sec["close"]
  ## sec["dpo_future_returns"] = sec["dpo"].shift(-future_horizon) - sec["dpo"]

