import pandas as pd
from features_candlestick import get_features_cnd
# 40in20out, volume spike
# for trend and setting stops
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

# %%
def get_features_viz(sec):
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
  sec_all["ts_dt_season_13"] = pd.Series(sec_all["date"].dt.year % 13).astype(str) + sec_all["date"].dt.strftime(":%m")
  sec_all["ts_dt_election"] = pd.Series(sec_all["date"].dt.year % 4).astype(str) + sec_all["date"].dt.strftime(":%m")
  # -- doy
  # sec_all[f"ts_dt_doy"] = sec_all["date"].dt.dayofyear
  sec_all[f"ts_dt_doy"] = sec_all["date"].dt.strftime("%m:%b:%d")
  # -- dom
  sec_all["ts_dt_dom"] = sec_all["date"].dt.day
  # -- dow
  # sec_all.loc[:, f"ts_dt_dow"] = sec_all["date"].dt.dayofweek
  sec_all[f"ts_dt_dow"] = sec_all["date"].dt.strftime("%w:%a")
