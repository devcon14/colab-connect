# colab-connect

Personal utility library for market data analysis on google colaboratory

* storing backtests and results for trading strategies I come across
* strategy visualisation and optimisation

```mermaid
graph TD
    A[Pandas Dataframes]
    B[Metatrader 5]
    C[Backtesting.py]
    D[Visualisation]
    E[Data]

    E-->|Broker,Bigquery|A
    A-->|Trade Live|B
    A-->|Backtest|C
    A-->|Optimise|D
```

