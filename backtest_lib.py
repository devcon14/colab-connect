def fill_empty_bt_columns(sec):
   if "signal_short" not in sec.columns:
      sec["signal_short"] = False
   if "signal_short_exit" not in sec.columns:
      sec["signal_short_exit"] = False
   if "signal_long_exit" not in sec.columns:
      sec["signal_long_exit"] = False
   if "signal_long" not in sec.columns:
      sec["signal_long"] = False

