import random
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import List


class Timeframe(Enum):
    ONE_MINUTE = '1m', int(60 * 1000)
    THREE_MINUTES = '3m', int(3 * 60 * 1000)
    FIVE_MINUTES = '5m', int(5 * 60 * 1000)
    FIFTEEN_MINUTES = '15m', int(15 * 60 * 1000)
    THIRTY_MINUTES = '30m', int(30 * 60 * 1000)
    ONE_HOUR = '1h', int(1 * 60 * 60 * 1000)
    TWO_HOURS = '2h', int(2 * 60 * 60 * 1000)
    FOUR_HOURS = '4h', int(4 * 60 * 60 * 1000)
    SIX_HOURS = '6h', int(6 * 60 * 60 * 1000)
    EIGHT_HOURS = '8h', int(8 * 60 * 60 * 1000)
    TWELVE_HOURS = '12h', int(12 * 60 * 60 * 1000)
    ONE_DAY = '1d', int(1 * 24 * 60 * 60 * 1000)
    THREE_DAYS = '3d', int(3 * 24 * 60 * 60 * 1000)
    ONE_WEEK = '1w', int(7 * 24 * 60 * 60 * 1000)
    ONE_MONTH = '1M', int(
        31 * 24 * 60 * 60 * 1000)  # TODO: implementation of 1 month is more complicated than this, assuming 31 days in a month here

    @property
    def code(self):
        return self.value[0]

    @property
    def milliseconds(self):
        return self.value[1]


class OrderStatus(Enum):
    NEW = 'NEW'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    FILLED = 'FILLED'
    CANCELED = 'CANCELED'
    PENDING_CANCEL = 'PENDING_CANCEL'
    REJECTED = 'REJECTED'
    EXPIRED = 'EXPIRED'


@dataclass
class SymbolInformation:
    minimum_quantity: float = 0.0
    maximum_quantity: float = 0.0
    quantity_step: float = 0.0
    price_step: float = 0.0
    minimum_price: float = 0.0
    maximum_price: float = 0.0
    minimal_cost: float = 0.0
    asset: str = ''
    symbol: str = ''


@dataclass
class Position:
    symbol: str
    entry_price: float
    position_size: float
    unrealizedProfit: float
    side: str
    initial_margin: float


@dataclass
class Income:
    symbol: str
    asset: str
    type: str
    income: float
    timestamp: float
    transaction_id: int


@dataclass
class Trade:
    symbol: str
    asset: str
    type: str
    timestamp: float
    order_id: int
    quantity: float
    price: float
    side: str


class OrderType(Enum):
    LIMIT = 'LIMIT'
    TAKE_PROFIT = 'TAKE_PROFIT'
    STOP = 'STOP'
    TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'
    STOP_MARKET = 'STOP_MARKET'
    TRAILING_STOP_MARKET = 'TRAILING_STOP_MARKET'
    MARKET = 'MARKET'
    LIQUIDATION = 'LIQUIDATION'


@dataclass
class Order:
    symbol: str = None
    quantity: float = None
    side: str = None
    position_side: str = None
    status: OrderStatus = None
    type: OrderType = None
    price: float = None


@dataclass
class AssetBalance:
    asset: str
    balance: float
    unrealizedProfit: float


@dataclass
class Balance:
    totalBalance: float
    totalUnrealizedProfit: float
    assets: List[AssetBalance] = field(default_factory=lambda: [])


@dataclass
class Tick:
    symbol: str
    price: float
    qty: float
    timestamp: int


@dataclass
class ScraperConfig:
    api_key: str = ''
    api_secret: str = ''
    exchange: str = ''
    test_net: bool = False
    symbols: List[str] = field(default_factory=lambda: [])
