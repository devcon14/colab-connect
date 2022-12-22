import pandas as pd
import numpy as np

# %%
# utility functions
def get_ft_level(price_level, signal_array, index=None):
  level = np.where(signal_array, price_level, None)
  level = pd.Series(level)
  level = level.ffill()
  if not index is None:
    # assigning failed where the dataframe has a date index
    # and the new series has a rangeindex
    # this allows for overriding the index
    level.index = index
  return level

def bt_convert_frame(sec):
  # prepare dataframe for backtest
  # sec.rename(columns={"open": "Open", "close": "Close", "high": "High", "low": "Low", "volume": "Volume"})
  bt_df = sec.copy()
  bt_df.columns = [col[0].upper() + col[1:] for col in bt_df.columns]
  bt_df.index = pd.to_datetime(bt_df.index)
  return bt_df

# %%
try:
  from backtesting import Backtest, Strategy
  from backtesting.lib import crossover
except Exception as e:
  # !pip install backtesting
  from backtesting import Backtest, Strategy
  from backtesting.lib import crossover

def fn_signal_long(): return sec["signal_long"]
def fn_signal_short(): return sec["signal_short"]

class StopEntryStrategy(Strategy):
  # one means exit on same day
  # len(data) == 1 on bar 0
  exit_delay = 1
  max_delay = 13

  def init(self):
    self.is_signal_long = self.I(fn_signal_long, overlay=False)
    self.is_signal_short = self.I(fn_signal_short, overlay=False)
    self.buy_stop = self.I(get_ft_level, self.data.High, self.is_signal_long.s)
    self.sell_stop = self.I(get_ft_level, self.data.Low, self.is_signal_short.s)

  def next(self):
    if not self.position:
      if self.is_signal_long:
        self.buy(stop=self.data["High"])
      elif self.is_signal_short:
        self.sell(stop=self.data["Low"])

    else:
      for trade in self.trades:
        if len(self.data) - trade.entry_bar >= self.max_delay:
            trade.close()
        if len(self.data) - trade.entry_bar >= self.exit_delay:
          if trade.pl > 0:
            trade.close()

def get_bt_results(bt_df):
  bt = Backtest(bt_df,
                StopEntryStrategy,
                cash=10000,
                trade_on_close=True,
                exclusive_orders=True,
                commission=0.001
                )

  output = bt.run()
  bt.plot()
  return bt

