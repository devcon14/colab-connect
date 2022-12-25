from features_candlestick import get_features_cnd
# 40in20out, volume spike
from features_candlestick import get_features_ta as get_features_cnd_trend

# win streaks, stats_ta, bizdays, dow_by_week
from features_kaggle import get_features as get_features_kaggle

# chart features, date features
# def get_all_features

# %%
def get_features_indicators(sec):
  import pandas_ta as ta
  # sec["ft_ta_stoch_p14"] = ta.stoch(close=sec.close, high=sec.high, low=sec.low, length=14)["STOCHk_14_3_3"]
  sec["ft_ta_rsi_p2"] = ta.rsi(close=sec.close, length=2)
  sec["ft_ta_sma_p200"] = sec["close"].rolling(200).mean()
