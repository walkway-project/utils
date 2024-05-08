from enum import Enum, unique

@unique
class Market(Enum):
    BINANCE_SPOT = 0
    BINANCE_USDT_FUTURES = 1

