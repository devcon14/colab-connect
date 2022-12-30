# colab-connect

My personal shared utility library for market data analysis on google colaboratory

* storing backtests for trading strategies I come across and want to try out
* strategy visualisation and optimisation

The architecture I'm aiming for:

```mermaid
graph TD
    A[Pandas Dataframes]
    B[Metatrader 5]
    C[Backtesting.py]
    D[Visualisation]

    A-->|Trade Live|B
    A-->|Backtest|C
    A-->|Optimise|D
```

