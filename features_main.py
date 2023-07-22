from numba import jit
import pandas as pd
from features_candlestick import get_features_cnd
# 40in20out, volume spike
# for trend and setting stops
from features_ta import get_features_ta as get_features_cnd_trend
from features_ta import get_features_combo

# win streaks, stats_ta, bizdays, dow_by_week
from features_kaggle import get_features as get_features_kaggle
from features_kaggle import get_features as get_features_kaggle

# chart features, date features
# def get_all_features

@jit(nopython=True)
def fn_nr_of(cr):
  """
  >>> sec["ft_cnd_nr_of"] = sec["ft_cnd_range"].rolling(CND_WINDOW).apply(fn_nr_of, engine="numba", raw=True)
  """
  nr_of = 0
  for index in range(len(cr)-1):
    if cr[-1] < cr[-index-2]:
      nr_of += 1
    else:
      return nr_of
  return nr_of
from numba import jit

# TODO wr_of for bank candles
def get_nr_val(x):
  """
  >>> sec["ft_cnd_range"].rolling(5).apply(get_nr_val)
  """
  for i in len(x):
    if x.iloc[-1] < x.iloc[-i]:
      n += 1
    else:
      return n

# %%
def get_features_indicators(sec, periods=[2]):
  import pandas_ta as ta
  sec["ft_ta_sma_p200"] = sec["close"].rolling(200).mean()
  
  for period in periods:
    # sec["ft_ta_stoch_p14"] = ta.stoch(close=sec.close, high=sec.high, low=sec.low, length=14)["STOCHk_14_3_3"]
    sec[f"ft_ta_rsi_p{period}"] = ta.rsi(close=sec.close, length=period)
    sec[f"ft_ta_kj_trend_up_p{period}"] = sec["close"] > sec["close"].shift(period)

# %%
def get_features_viz_tt(sec):
  from finta import TA
  df = TA.WILLIAMS_FRACTAL(sec, period=2)
  # print (df)
  sec["viz_bearish_fractal"] = df["BearishFractal"]
  sec["viz_bullish_fractal"] = df["BullishFractal"]
    
# %%
def get_features_viz(sec, prefix="ts", tt=True):
  # must apply with 1 symbol
  sec["change"] = sec["close"].pct_change()

  # can apply with all
  sec_all = sec
  sec_all["date"] = pd.to_datetime(sec_all.index)

  # -- YM
  sec_all[f"{prefix}_dt_YM"] = sec_all["date"].dt.strftime("%Y:%m")
  # -- cycles
  sec_all[f"{prefix}_dt_election"] = pd.Series(sec_all["date"].dt.year % 4).astype(str) + sec_all["date"].dt.strftime(":%m")
  sec_all[f"{prefix}_dt_shmita"] = pd.Series(sec_all["date"].dt.year % 7).astype(str) + sec_all["date"].dt.strftime(":%m")
  sec_all[f"{prefix}_dt_season_9"] = pd.Series(sec_all["date"].dt.year % 9).astype(str) + sec_all["date"].dt.strftime(":%m")
  sec_all[f"{prefix}_dt_decennial"] = pd.Series(sec_all["date"].dt.year % 10).astype(str) + sec_all["date"].dt.strftime(":%m")
  sec_all[f"{prefix}_dt_season_13"] = pd.Series(sec_all["date"].dt.year % 13).astype(str) + sec_all["date"].dt.strftime(":%m")
  # armstrong
  try:
    DATE_FIELD = "date"
    as_cycle_months = 8.615384615*12
    as_num = sec_all[DATE_FIELD].dt.year * 12 + sec_all[DATE_FIELD].dt.month
    sec_all[f"{prefix}_dt_as_cycle"] = (as_num % as_cycle_months).astype(int)
  except:
    print ("armstrong failed")
  
  # -- doy
  # sec_all[f"{prefix}_dt_doy"] = sec_all["date"].dt.dayofyear
  sec_all[f"{prefix}_dt_doy"] = sec_all["date"].dt.strftime("%m:%b:%d")
  # -- dom
  sec_all[f"{prefix}_dt_dom"] = sec_all["date"].dt.day
  # -- dow
  # sec_all.loc[:, f"{prefix}_dt_dow"] = sec_all["date"].dt.dayofweek
  sec_all[f"{prefix}_dt_dow"] = sec_all["date"].dt.strftime("%w:%a")
  
def get_features_for_symbol(sec, symbol):
  # features that are specific to a symbol, like quarter theory / binning
  if symbol == "SPY":
    sec.loc[sec.symbol==symbol, "ft_qtt_100"] = sec["close"].apply(lambda x: int(x % 100 / 10) * 10)
    sec.loc[sec.symbol==symbol, "ft_qtt_10"] = sec["close"].apply(lambda x: int(x % 10))
    sec.loc[sec.symbol==symbol, 'ft_ta_dspi_p200_bin'] = sec['ft_ta_dspi_p200'].fillna(0).apply(lambda x: int(x/10.0) * 10.0)

  if tt:
    get_features_viz_tt(sec)
  
# %%
from features_kaggle import bizday, dow_month_code as wom_for_dow
import pandas as pd

def get_features_dttm(sec):
  get_features_viz(sec, prefix="ft", tt=True)
  # sec["date"] = pd.to_datetime(sec.index)
  
  sec.loc[:, f"ft_dt_bizday-n"] = sec["date"].map(lambda x: bizday(x))
  sec.loc[:, f"ft_dt_bizday+n"] = sec["date"].map(lambda x: bizday(x, reverse=False))

  sec.loc[:, "ft_dt_dow_wom"] = sec["date"].map(lambda x: f"{x.dayofweek}:{wom_for_dow(x)}")
  sec['ft_dt_NFP'] = sec['ft_dt_dow_wom'].replace("5:0",1)
  sec['ft_dt_options_expiry'] = sec['ft_dt_dow_wom'].replace("5:4",1)

def fix_ml_columns(sec, first_run=True):
  if first_run:
    del sec["ft_dt_YM"]
    sec["ft_dt_doy"] = sec["ft_dt_doy"].apply(lambda x: int(x[-2:]))
    sec["ft_dt_dow"] = sec["ft_dt_dow"].apply(lambda x: int(x[0]))

  for col in ["ft_dt_election", "ft_dt_shmita", "ft_dt_season_9", "ft_dt_decennial", "ft_dt_season_13"]:
    sec[col] = sec[col].astype("category")

  for col in ["ft_dt_NFP", "ft_dt_options_expiry"]:
    sec[col] = sec[col].astype("category")

  for col in ["ft_dt_dow_wom", "ft_dt_dow_wom_code"]:
    sec[col] = sec[col].astype("category")

  print (sec.columns)

class Feat:

  def features_bt(sec, future_horizon=5):
    """
    """
    from features_backtest import backtest_metrics
    backtest_metrics(sec, future_horizon = future_horizon)

  def fn_nr_of(cr):
    """
    >>> sec["ft_cnd_nr_of"] = sec["ft_cnd_range"].rolling(CND_WINDOW).apply(fn_nr_of, engine="numba", raw=True)
    """
    return fn_nr_of(cr)

  def get_features_combo(sec, indicators, periods):
    """
    >>> get_features_combo(sec, ["ft_ta_donchian"], [14, 50, 100])
    """
    from features_ta import get_features_combo
    return get_features_combo(sec, indicators, periods)

  def rsi(df, periods = 14, ema = True):
      """
      Returns a pd.Series with the relative strength index.
      # https://www.roelpeters.be/many-ways-to-calculate-the-rsi-in-python-pandas/
      """
      close_delta = df['close'].diff()

      # Make two series: one for lower closes and one for higher closes
      up = close_delta.clip(lower=0)
      down = -1 * close_delta.clip(upper=0)

      if ema == True:
        # Use exponential moving average
          ma_up = up.ewm(com = periods - 1, adjust=True, min_periods = periods).mean()
          ma_down = down.ewm(com = periods - 1, adjust=True, min_periods = periods).mean()
      else:
          # Use simple moving average
          ma_up = up.rolling(window = periods, adjust=False).mean()
          ma_down = down.rolling(window = periods, adjust=False).mean()

      rsi = ma_up / ma_down
      rsi = 100 - (100/(1 + rsi))
      return rsi

  # compare dashlets_v3:Utils
  # from smash days
  def add_level_pd(sec, name, signal_key, price_key):
    """
    Usually private method
    See add_levels as easier to use endpoint
    """
    import numpy as np
    sec[f"ft_{name}_level"] = np.where(sec[signal_key], sec[price_key], None)
    sec[f"ft_{name}_level"] = sec[f"ft_{name}_level"].ffill()
    sec[f"ft_{name}_level_{price_key}_pd"] = sec["close"] / sec[f"ft_{name}_level"]
    # bt or chart
    sec[f"chart_{name}_level_{price_key}"] = sec[f"ft_{name}_level"]
    del sec[f"ft_{name}_level"]

  def add_levels(sec, level_list, feature_type, col="close"):
    """
    >>> sec["ft_dt_hkgap_begin"] = sec["datetime_local"].dt.hour == 21
    >>> sec["ft_dt_hkgap_end"] = sec["datetime_local"].dt.hour == 2
    >>> Feat.add_levels(sec, ["hkgap_begin", "hkgap_end"], feature_type="ft_dt", col="close")

    Returns:
    f"ft_{name}_level": Price level
    f"ft_{name}_level_{price_key}_pd"
    """
    for name in level_list:
      Feat.add_level_pd(sec, name, f"{feature_type}_{name}", col)

  def bizdays(df):
    """
    """
    from features_kaggle import bizday
    df.loc[:, f"ft_dt_bizday-n"] = df["datetime_local"].map(lambda x: bizday(x))
    df.loc[:, f"ft_dt_bizday+n"] = df["datetime_local"].map(lambda x: bizday(x, reverse=False))

  def dow_counter(df):
    """
    5:4 is fourth Friday of month.
    5:0 is first Friday.
    """
    from features_kaggle import dow_month_code
    df.loc[:, "ft_dt_dow_month_code"] = df["datetime_local"].map(lambda x: f"{x.dayofweek}:{dow_month_code(x)}")
    # TODO interesting as a levels feature
    df['ft_dt_nfp'] = df['ft_dt_dow_month_code'].replace("5:0",1)
    df['ft_dt_options_expiry'] = df['ft_dt_dow_month_code'].replace("5:4",1)

if __name__ == "__main__":
  get_features_dttm(sec)
