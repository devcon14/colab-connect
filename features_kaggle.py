# %%
import pandas as pd
import numpy as np
from datetime import date
from numba import jit

# https://stackoverflow.com/questions/18618288/how-do-i-convert-dates-into-iso-8601-datetime-format-in-a-pandas-dataframe
if False:
    df = train
    # mask = (df["High"] != df["Low"]) & (df.Date == current_date)
    mask = (df["High"] != df["Low"]) & (df.Date == "2017-01-04")
    # pd.to_datetime(df['q_date']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

@jit(nopython=True)
def win_streak_nb(x):
    streak = 0
    for i in range(len(x)):
        one = x[-(i+1)]
        two = x[-(i+2)]
        if one > two:
            streak += 1
        else:
            return streak
    return streak

@jit(nopython=True)
def loss_streak_nb(x):
    streak = 0
    for i in range(len(x)):
        one = x[-(i+1)]
        two = x[-(i+2)]
        if one < two:
            streak += 1
        else:
            return streak
    return streak

if __name__ == "__main__":
    sec["ft_ta_win_streak"] = sec["close"].rolling(10).apply(win_streak_nb, engine="numba", raw=True)
    sec["ft_ta_loss_streak"] = sec["close"].rolling(10).apply(loss_streak_nb, engine="numba", raw=True)

# %%
dow_month_cache = {}
def dow_month_code(date):
    if date.month == 12:
        start_ym = f"{date.year}-12"
        end_ym = f"{date.year+1}-01"
    else:
        start_ym = f"{date.year}-{date.month}"
        end_ym = f"{date.year+1}-{date.month+1}"
    
    weekday = date.weekday()
    if weekday == 0:
        freq = "W-MON"
    elif weekday == 1:
        freq = "W-TUE"
    elif weekday == 2:
        freq = "W-WED"
    elif weekday == 3:
        freq = "W-THU"
    elif weekday == 4:
        freq = "W-FRI"
    elif weekday == 5:
        freq = "W-SAT"
    elif weekday == 6:
        freq = "W-SUN"
    else:
        crash
    
    str_fmt = "%Y-%m-%d"
    cache_code = f"{date.year}:{date.month}:{weekday}"
    if cache_code not in dow_month_cache:
        days = pd.date_range(f"{start_ym}", f"{end_ym}", freq=freq)
        dow_month_cache[cache_code] = days_list = [x.strftime(str_fmt) for x in days]
    
    days_list = dow_month_cache[cache_code]
    date_str = date.strftime(str_fmt)
    
    if date_str in days_list:
        day_count = days_list.index(date_str)
        reverse = False
        if reverse:
            return day_count - len(days_list)
        else:
            return day_count
    else:
        return 0

if __name__ == "__main__":
    # prices["Date"].map(lambda x: dow_month_code(x))
    prices.loc[:, "ft_dt_dow_month_code"] = prices["Date"].map(lambda x: f"{x.dayofweek}:{dow_month_code(x)}")
    df['NFP'] = df_mg['ft_dt_dow_month_code'].replace("5:0",1)
    df['options_expiry'] = df_mg['ft_dt_dow_month_code'].replace("5:4",1)
    prices

# %%
def bizday(dt, reverse = True):
    if dt.dayofweek in [5, 6]:
        return None
    
    if reverse:
        return np.busday_count(date(dt.year, dt.month, dt.daysinmonth), dt.date())
    else:
        return np.busday_count(date(dt.year, dt.month, 1), dt.date())

if __name__ == "__main__":
    df.loc[:, f"ft_dt_bizday-n"] = df["Date"].map(lambda x: bizday(x))
    df.loc[:, f"ft_dt_bizday+n"] = df["Date"].map(lambda x: bizday(x, reverse=False))

# %%
def stats_ta(df, series, prefix, period, add_zscore=False):
    # faster: https://stackoverflow.com/questions/47164950/compute-rolling-z-score-in-pandas-dataframe
    # cheating on shift and ddof
    # https://marceldietsch.medium.com/technical-tuesday-exploring-financial-market-data-with-z-scores-in-python-476be16406a8
    r = series.rolling(period)
    m = r.mean()
    s = r.std()
    z = (series-m)/s
    dsp = (series-m)/series
    df[f'{prefix}_dsp_{period}D'] = dsp
    if add_zscore:
        df[f'{prefix}_zscore_{period}D'] = z
        df[f'{prefix}_std_{period}D'] = s

# %%
def get_features(sec):
    sec["ft_ta_win_streak"] = sec["close"].rolling(10).apply(win_streak_nb, engine="numba", raw=True)
    sec["ft_ta_loss_streak"] = sec["close"].rolling(10).apply(loss_streak_nb, engine="numba", raw=True)
    sec.loc[:, f"ft_dt_bizday-n"] = sec["date"].map(lambda x: bizday(x))
    sec.loc[:, f"ft_dt_bizday+n"] = sec["date"].map(lambda x: bizday(x, reverse=False))
    sec.loc[:, "ft_dt_dow_wom_code"] = sec["date"].map(lambda x: f"{x.dayofweek}:{dow_month_code(x)}")
    # stats_ta(sec, sec["close"], "ft_ta", 14)
    
        
