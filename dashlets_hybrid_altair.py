import pandas as pd
import altair as alt

class Dash:

  def aggregate(sec, kdims, target, max_x_bins=None, max_y_bins=None, min_count=5):
    if max_x_bins:
      grpby_x = pd.cut(sec[kdims[0]], max_x_bins, labels=False)
    else:
      grpby_x = sec[kdims[0]]

    if len(kdims) == 2:
      if max_y_bins:
        grpby_y = pd.cut(sec[kdims[1]], max_y_bins, labels=False)
      else:
        grpby_y = sec[kdims[1]]

    if len(kdims)==1:
      grp = sec.groupby([grpby_x])
    elif len(kdims)==2:
      grp = sec.groupby([grpby_x, grpby_y])

    agg = grp[target].agg(
        mean="mean", count="count", median="median",
        std="std",
        p75=lambda x: x.quantile(0.75),
        winrate=lambda x: (x > 0).sum() / x.count()
        ).reset_index()
    
    agg_fn = "mean"
    agg[f"{agg_fn}_cumprod"] = agg[agg_fn].add(1).cumprod().sub(1)    
    agg[f"{agg_fn}_cumsum"] = agg[agg_fn].cumsum()
    agg[f"{agg_fn}_str"] = agg[agg_fn].apply(lambda x: f"{x:.6f}")

    agg = agg[agg["count"] > min_count]
    Dash.agg = agg

    return agg

  def chart_ts(sec, x_field, target, type=""):
    """
    Line Chart:
    >>> Dash.chart_ts(sec, field_a, target)
    >>> Dash.chart_ts(sec, field_a, target, "_cumsum")
    """
    agg = Dash.aggregate(sec, [x_field], target, None, None, min_count=0)

    return alt.Chart(agg).mark_line().encode(
        x=x_field,
        y=f'mean{type}',
    )

  def chart_opt(sec, kdims, target, max_x_bins=None, max_y_bins=None, min_count=5):
    """
    Set max_x/y_bins to None to disable binning.

    Barchart:
    >>> Dash.chart_opt(sec, [field_a], target, None).interactive()
    Heatmap:
    >>> Dash.chart_opt(sec, [field_a, field_b], target, None).interactive()
    """
    agg = Dash.aggregate(sec, kdims, target, max_x_bins, max_y_bins, min_count)

    if len(kdims)==1:
      base = alt.Chart(agg).mark_bar().encode(
          y='mean',
      )
    elif len(kdims)==2:
      # base = alt.Chart(agg).mark_point(size=15**2, filled=True).encode(
      base = alt.Chart(agg).mark_rect().encode(
        y=f"{kdims[1]}:O",
        color=alt.Color('mean') # .scale(scheme='magma'),
      )

    return base.encode(
        x=f"{kdims[0]}:O",
        tooltip=['count','mean','median','winrate']
    )
