# %%
try:
  from backtesting import Backtest, Strategy
except Exception as e:
  # !pip install backtesting
  from backtesting import Backtest, Strategy

class MarketStrategy(Strategy):

  def init(self):

    def fn_signal_long(): return self.data.Signal_long
    def fn_signal_short(): return self.data.Signal_short

    self.is_signal_long = self.I(fn_signal_long, overlay=False)
    self.is_signal_short = self.I(fn_signal_short, overlay=False)

  def signal_exit(self):
    if self.position:
      
      for trade in self.trades:
        if trade.is_long:
          if self.data.Signal_long_exit:
            trade.close()
        elif trade.is_short:
          if self.data.Signal_short_exit:
            trade.close()

  def postprocess(self):
    self.signal_exit()
        
  def next(self):

    if not self.position:
      
      if self.data.Signal_long:
        self.buy()
      elif self.data.Signal_short:
        self.sell()
    
    self.postprocess()

