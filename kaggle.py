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
