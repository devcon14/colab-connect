import pandas as pd
import altair as alt

class Dash:

  # Interval (Object with .left and .right properties)
  LABEL_METHOD = None
  # integer index
  # LABEL_METHOD = False
  # custom, eg.
  # LABEL_METHOD = ["low", "medium", "high"]
  def agg_target(grp, target, window_agg_col, drop_kdims=False):
    """
    private
    """
    agg = grp[target].agg(
        mean="mean",
        median="median",
        count="count",
        std="std",
        # p75=lambda x: x.quantile(0.75),
        winrate=lambda x: (x > 0).sum() / x.count()
        ).reset_index()

    if drop_kdims:
      # slice from column mean onwards
      agg = agg.loc[:, "mean":]

    agg = agg.rename(columns={
        "mean": f"{target}_mean",
        "count": f"{target}_count",
        "median": f"{target}_median",
        "std": f"{target}_std",
        "winrate": f"{target}_winrate"
        })
    # agg.columns = [f"{target}_{x}" for x in agg.columns]

    # cumulative/rolling functions could be a secondary override function
    # executed after completing aggregation on select columns
    # now with the transform_window function in altair this isn't as important
    WINDOW_TRANSFORM = False
    if WINDOW_TRANSFORM:
      agg[f"{target}_{window_agg_col}_cumsum"] = agg[f"{target}_{window_agg_col}"].cumsum()
      agg[f"{target}_{window_agg_col}_cumprod"] = agg[f"{target}_{window_agg_col}"].add(1).cumprod().sub(1)
      agg[f"{target}_{window_agg_col}_str"] = agg[window_agg_col].apply(lambda x: f"{x:.6f}")

    return agg

  def create_bins(sec, kdims, max_x_bins, max_y_bins):

    # https://pandas.pydata.org/docs/reference/api/pandas.Interval.html
    if max_x_bins:
      grpby_x = pd.cut(sec[kdims[0]], max_x_bins, labels=Dash.LABEL_METHOD)
      if Dash.LABEL_METHOD is None:
        grpby_x = grpby_x.apply(lambda x: x.right)
    else:
      grpby_x = sec[kdims[0]]

    grpby_y = None
    if len(kdims) == 2:
      if max_y_bins:
        grpby_y = pd.cut(sec[kdims[1]], max_y_bins, labels=Dash.LABEL_METHOD)
        if Dash.LABEL_METHOD is None:
          grpby_y = grpby_y.apply(lambda x: x.right)
      else:
        grpby_y = sec[kdims[1]]

    return grpby_x, grpby_y

  # TODO kdims=["symbol", ...], max_bins=[None, 3, 5], targets=[...]
  def im_aggregate(sec, kdims, target, max_x_bins=None, max_y_bins=None, min_count=5, other_targets=[], symbol_keys = ["symbol"], window_agg_col="mean"):

    grpby_x, grpby_y = Dash.create_bins(sec, kdims, max_x_bins, max_y_bins)

    # NOTE we group by symbol as the field name
    # with grpby_x/y a series or binned series is passed
    if len(kdims)==1:
      grp = sec.groupby(symbol_keys + [grpby_x])
    elif len(kdims)==2:
      grp = sec.groupby(symbol_keys + [grpby_x, grpby_y])

    agg = Dash.agg_target(grp, target, window_agg_col)
    agg = agg[agg[f"{target}_count"] > min_count]
    Dash.agg = agg

    aggs = []
    aggs.append(agg)
    for _target in other_targets:
      agg = Dash.agg_target(grp, _target, window_agg_col, drop_kdims=True)
      agg = agg[agg[f"{_target}_count"] > min_count]
      aggs.append(agg)
    Dash.agg_list = aggs
    Dash.aggs = pd.concat(aggs, axis="columns")

    return Dash.aggs
  
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

  def chart_im_ts(sec, x_field, target, type=""):
    """
    un-implemented
    """
    pass

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
