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

# def fn_signal_long(): return sec["signal_long"]
# def fn_signal_short(): return sec["signal_short"]

class StopEntryStrategy(Strategy):
  # one means exit on same day
  # len(data) == 1 on bar 0
  exit_delay = 1
  max_delay = 13

  def init(self):

    def fn_signal_long(): return self.data.Signal_long
    def fn_signal_short(): return self.data.Signal_short

    self.is_signal_long = self.I(fn_signal_long, overlay=False)
    self.is_signal_short = self.I(fn_signal_short, overlay=False)
    self.buy_stop = self.I(get_ft_level, self.data.High, self.data.Signal_long, overlay=True)
    self.sell_stop = self.I(get_ft_level, self.data.Low, self.data.Signal_short, overlay=True)

  def timed_exit(self, on_profit=False):
    if self.position:
      for trade in self.trades:
        if len(self.data) - trade.entry_bar >= self.max_delay:
            trade.close()
        if len(self.data) - trade.entry_bar >= self.exit_delay:
          if not on_profit:
            trade.close()
          elif on_profit and trade.pl > 0:
            trade.close()
    
  def postprocess(self):
    self.timed_exit()
    
  def next(self):
    if not self.position:
      if self.is_signal_long:
        self.buy(stop=self.data["High"])
      elif self.is_signal_short:
        self.sell(stop=self.data["Low"])
        
    self.postprocess()

def get_bt_results(sec, StrategyClass=StopEntryStrategy):
  bt_df = bt_convert_frame(sec)
  bt = Backtest(bt_df,
                StrategyClass,
                cash=10000,
                trade_on_close=True,
                exclusive_orders=True,
                commission=0.001
                )

  output = bt.run()
  bt.plot()
  return bt

