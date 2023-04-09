import pandas as pd
from features_candlestick import get_features_cnd
# 40in20out, volume spike
# for trend and setting stops
from features_ta import get_features_ta as get_features_cnd_trend

# win streaks, stats_ta, bizdays, dow_by_week
from features_kaggle import get_features as get_features_kaggle
from features_kaggle import get_features as get_features_kaggle

# chart features, date features
# def get_all_features

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
  
if __name__ == "__main__":
  get_features_dttm(sec)
