import pandas as pd
import numpy as np
from panel import interact

def get_prior_return_metrics(sec):
  """
  Applicable as features or for charting.
  Includes gap and session returns.
  """
  # today's close - yesterday's close
  sec["change"] = sec["close"].pct_change()
  sec["returns"] = sec["close"] - sec["close"].shift(1)
  sec["session_returns"] = sec["close"] - sec["open"]
  # today's open - yesterday's close
  sec["gap_returns"] = sec["open"] - sec["close"].shift(1)

def prior_to_future_metrics(sec, future_horizon):
  if future_horizon == 1:
    suffix = ""
  else:
    suffix = f"_p{future_horizon}"
    
  sec[f"future_change{suffix}"] = sec["change"].shift(-future_horizon)
  sec[f"future_returns{suffix}"] = sec["returns"].shift(-future_horizon)
  
def backtest_metrics(sec, future_horizon = 5, adj=None):
  """
  Future returns for backtesting.
  """
  get_prior_return_metrics(sec)
  prior_to_future_metrics(sec, future_horizon)
  
  if adj:
    sec["future_returns"] *= adj

  # ternary is for rolling window exceptions
  sec["mae"] = sec["close"].rolling(future_horizon).apply(lambda x: x[0] - np.min(x) if len(x) >= future_horizon else 0)
  sec["mae"] = sec["mae"].shift(-future_horizon)
  sec[f"future_mae_p{future_horizon}"] = sec["mae"]
  
  sec["mfe"] = sec["close"].rolling(future_horizon).apply(lambda x: np.max(x) - x[0] if len(x) >= future_horizon else 0)
  sec["mfe"] = sec["mfe"].shift(-future_horizon)
  sec[f"future_mfe_p{future_horizon}"] = sec["mae"]

  sec["f2a"] = sec["mfe"] - sec["mae"]
  sec["asymmetry"] = np.abs(sec["f2a"])
  sec[f"future_f2a_p{future_horizon}"] = sec["f2a"]

  return sec

def interactable_backtest_metrics(sec_inst, future_horizon = 5, adj=None):
  sec_tr = sec_inst.sec_all.groupby("symbol").apply(lambda x: get_backtest_metrics_on_df(x, future_horizon, adj))  
  sec_inst.sec_all = sec_tr
  return sec_tr[["long_mfe", "long_mae", "long_f2a"]].head(5)

if __name__ == "__main__":
  adj = 1000
  dashlet_backtest_metrics = interact(interactable_backtest_metrics, sec_inst=universe, future_horizon=(1, 20))
  display(dashlet_backtest_metrics)

  # sec_inst.sec_all.groupby("symbol").apply(lambda x: backtest_metrics(x, 5))
