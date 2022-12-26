def crossunder(arr, val):
  return (arr.shift(1) > val) & (arr < val)

def crossover(arr, val):
  return (arr.shift(1) < val) & (arr > val)

def fill_empty_bt_columns(sec):
   if "signal_short" not in sec.columns:
      sec["signal_short"] = False
   if "signal_short_exit" not in sec.columns:
      sec["signal_short_exit"] = False
   if "signal_long_exit" not in sec.columns:
      sec["signal_long_exit"] = False
   if "signal_long" not in sec.columns:
      sec["signal_long"] = False

# %%
def plot_gbm_importance(sec):
  import lightgbm as lgb

  # del sec["ft_low_past_40"]

  # fillna(False/0)
  x_train = sec[[x for x in sec.columns if "ft_" in x]]
  # x_train = x_train.fillna(value={"ft_rising_IRB_1": False})
  x_train = x_train.fillna(method="bfill")
  y_train = sec.future_returns

  # probably need high max depth to use man boolean features
  # model = lgb.LGBMRegressor(boosting_type='gbdt', max_depth=2, learning_rate=0.2, n_estimators=2000, seed=42)
  model = lgb.LGBMRegressor()
  model.fit(x_train, y_train) # , categorical_feature=cat_feat)

  # https://lightgbm.readthedocs.io/en/latest/pythonapi/lightgbm.plot_tree.html
  from lightgbm import plot_importance
  # model.feature_importances_
  # importance_type: gain or split
  plot_importance(
      model,
      # importance_type="split"
      )

if __name__ == "__main__":
  plot_gbm_importance(sec)
