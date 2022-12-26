# from https://github.com/jesse-ai/jesse/blob/master/jesse/utils.py
import math

def floor_with_precision(num: float, precision: int = 0) -> float:
    temp = 10 ** precision
    return math.floor(num * temp) / temp

def risk_to_qty(capital: float, risk_per_capital: float, entry_price: float, stop_loss_price: float, precision: int = 8,
                fee_rate: float = 0) -> float:
    """
    a risk management tool to quickly get the qty based on risk percentage
    :param capital:
    :param risk_per_capital:
    :param entry_price:
    :param stop_loss_price:
    :param precision:
    :param fee_rate:
    :return: float
    """
    risk_per_qty = abs(entry_price - stop_loss_price)
    size = risk_to_size(capital, risk_per_capital, risk_per_qty, entry_price)

    if fee_rate != 0:
        size = size * (1 - fee_rate * 3)

    return size_to_qty(size, entry_price, precision=precision, fee_rate=fee_rate)

def size_to_qty(position_size: float, entry_price: float, precision: int = 3, fee_rate: float = 0) -> float:
    """
    converts position-size to quantity
    example: requesting $100 at the entry_price of $50 would return 2
    :param position_size: float
    :param entry_price: float
    :param precision: int
    :param fee_rate:
    :return: float
    """
    # make sure entry_price is not None
    if entry_price is None:
        raise TypeError(f"entry_price is None")

    if math.isnan(position_size) or math.isnan(entry_price):
        raise TypeError(f"position_size: {position_size}, entry_price: {entry_price}")

    if fee_rate != 0:
        position_size *= 1 - fee_rate * 3

    return floor_with_precision(position_size / entry_price, precision)

def risk_to_size(capital_size: float, risk_percentage: float, risk_per_qty: float, entry_price: float) -> float:
    """
    calculates the size of the position based on the amount of risk percentage you're willing to take
    example: round(risk_to_size(10000, 1, 0.7, 8.6)) == 1229
    :param capital_size:
    :param risk_percentage:
    :param risk_per_qty:
    :param entry_price:
    :return: float
    """
    if risk_per_qty == 0:
        raise ValueError('risk cannot be zero')

    risk_percentage /= 100
    temp_size = ((risk_percentage * capital_size) / risk_per_qty) * entry_price
    return min(temp_size, capital_size)

