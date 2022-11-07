from backtesting import Backtest, Strategy
from backtesting.lib import crossover

def Highest(values, n):
  return pd.Series(values).rolling(n).max()

def Lowest(values, n):
  return pd.Series(values).rolling(n).min()

class TimedImpl:
  timed_exit_period = 20

  def timed_exit(self):
    if self.timed_exit_period:
      trade_length = len(self.data) - self.entry_bar
      if trade_length >= self.timed_exit_period:
        self.position.close()

class BailoutImpl:
  bailout_begin = 10
  bailout_pnl = 0.0
  
  def bailout_exit(self):
    if self.position:
      trade_length = len(self.data) - self.entry_bar
      if trade_length >= self.bailout_begin:
        if self.position.pl > self.bailout_pnl:
          self.position.close()
  
class OpenAndCloseImpl:

  def long_on_signal(self):
    if not self.position and self.data["Signal_long"]:
      self.entry_bar = len(self.data)
      params = self.get_long_params()
      self.buy(**params)

  def short_on_signal(self):
    if not self.position and self.data["Signal_short"]:
      self.entry_bar = len(self.data)
      params = self.get_short_params()
      self.sell(**params)

class StopAndReverseImpl:

  def long_on_signal(self):
    if self.data["Signal_long"]:
      self.entry_bar = len(self.data)
      params = self.get_long_params()
      self.buy(**params)

  def short_on_signal(self):
    if self.data["Signal_short"]:
      self.entry_bar = len(self.data)
      params = self.get_short_params()
      self.sell(**params)

class BracketImpl:
  R = 1.5
  USE_STOPS = True
  USE_STOP_ENTRY = False
  STOP_PCT = 0.08

  def get_long_params(self):
    params = {}
    if self.USE_STOPS:
      params["sl"] = self.calc_sl_pct("long", self.STOP_PCT)
      params["tp"] = self.calc_tp_R("long", self.R, params["sl"])
    # if self.USE_STOP_ENTRY:
    if hasattr(self, "calc_stop_entry"):
      params["stop"] = self.calc_stop_entry("long")
    return params

  def get_short_params(self):
    params = {}    
    if self.USE_STOPS:
      params["sl"] = self.calc_sl_pct("short", self.STOP_PCT)
      params["tp"] = self.calc_tp_R("short", self.R, params["sl"])
    # if self.USE_STOP_ENTRY:
    if hasattr(self, "calc_stop_entry"):
      params["stop"] = self.calc_stop_entry("short")
    return params

  def calc_sl_pct(self, side="long", percent=0.02):
      # TODO proper stop entries, and monitor open orders instead of open positions
      # https://kernc.github.io/backtesting.py/doc/backtesting/backtesting.html#backtesting.backtesting.Order&gsc.tab=0
    price = self.data.Close[-1]
    if side=="long":
      return (1.0 - percent) * price
    else:
      return (1.0 + percent) * price

  def calc_tp_R(self, side="long", R=1.5, stop_price=None):
    # return None
    price = self.data.Close[-1]
    stop_size = abs(price - stop_price)
    if side=="long":
      return price + R * stop_size
    else:
      return price - R * stop_size

  def calc_sl_candle(self, side="long", stop_adjust=0.0, index = -2):
    if side=="long":
      return max(0, self.data.Low[index]-stop_adjust)
    else:
      return self.data.High[index]+stop_adjust

  def _calc_stop_entry(self, side="long"):
    if side=="long":
      return self.data.High
    elif side=="short":
      return self.data.Low
    else:
      crash


class SimpleBase(Strategy):
  highest_period = 20
  lowest_period = 20

  def init(self):
    self.entry_bar = 0
    self.i_highest = self.I(Highest, self.data.High, self.highest_period)
    self.i_lowest = self.I(Lowest, self.data.Low, self.lowest_period)

  def get_long_params(self):
    return {}

  def get_short_params(self):
    return {}


class StopAndReverse(SimpleBase, StopAndReverseImpl):

  def next(self):
    self.long_on_signal()
    self.short_on_signal()

class TimedStrategy(SimpleBase, TimedImpl, OpenAndCloseImpl):

  def next(self):
    self.long_on_signal()
    self.short_on_signal()
    self.timed_exit()

class BailoutStrategy(SimpleBase, TimedImpl, BailoutImpl, OpenAndCloseImpl):

  def next(self):
    self.long_on_signal()
    self.short_on_signal()
    self.bailout_exit()
    self.timed_exit()

# stop order test
class BreakoutStrategy(Strategy):
  highest_period = 10
  lowest_period = 10

  def init(self):
    self.entry_bar = 0
    self.i_highest = self.I(Highest, self.data.High, self.highest_period)
    self.i_lowest = self.I(Lowest, self.data.Low, self.lowest_period)

  def next(self):
    for order in self.orders:
      order.cancel()
    self.buy(stop=self.i_highest)
    # it closes the last position when the next one buys

    if False:
      for trade in self.trades:
        print (trade.entry_bar, len(self.data))
        print (trade.entry_price, self.data["High"])

# %%
if False:
  import numpy as np

  stats = bt.optimize(
      # timed_exit_period=range(3, 30),
      # bailout_exit_period=range(1, 20),
      bailout_pnl=range(0, 200, 50),
      # R=list(np.arange(1.0, 5.5, 0.5)),
      # maximize='Equity Final [$]',
      maximize='Profit Factor',
      # constraint=lambda param: param.bailout_exit_period < param.timed_exit_period
      )

  print (stats._strategy)
  print (stats)
