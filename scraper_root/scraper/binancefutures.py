import datetime
import logging
import os
import threading
import time
from random import randint
from typing import List

from unicorn_binance_rest_api import BinanceRestApiManager
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

from scraper_root.scraper.data_classes import AssetBalance, Position, ScraperConfig, Tick, Balance, \
    Income, Order, Account
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


def is_asset_usd_or_derivative(asset: str):
    return asset.lower() in ["usdt", "busd", "usd", "usdc"]


class BinanceFutures:
    def __init__(self, account: Account, symbols: List[str], repository: Repository,
                 exchange: str = "binance.com-futures"):
        print('Binance initialized')
        self.account = account
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.repository = repository
        self.ws_manager = BinanceWebSocketApiManager(exchange=exchange, throw_exception_if_unrepairable=True,
                                                     warn_on_update=False)
        self.rest_manager = BinanceRestApiManager(self.api_key, api_secret=self.secret)

        self.rest_manager.FUTURES_URL = f"{os.getenv('FUTURES_URL', 'https://fapi.binance.com')}/fapi"
        self.rest_manager.FUTURES_DATA_URL = f"{os.getenv('FUTURES_DATA_URL', 'https://fapi.binance.com')}/futures/data"
        self.rest_manager.FUTURES_COIN_URL = f"{os.getenv('FUTURES_DATA_URL', 'https://fapi.binance.com')}/fapi"
        self.rest_manager.FUTURES_COIN_DATA_URL = f"{os.getenv('FUTURES_DATA_URL', 'https://dapi.binance.com')}/futures/data"

        self.tick_symbols = []

    def start(self):
        logger.info('Starting binance futures scraper')

        for symbol in self.symbols:
            symbol_trade_thread = threading.Thread(
                name=f'trade_thread_{symbol}', target=self.process_trades, args=(symbol,), daemon=True)
            symbol_trade_thread.start()

        sync_balance_thread = threading.Thread(
            name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        sync_trades_thread = threading.Thread(
            name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

        sync_orders_thread = threading.Thread(
            name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
        sync_orders_thread.start()

    def sync_trades(self):
        max_fetches_in_cycle = 3
        first_trade_reached = False
        while True:
            try:
                counter = 0
                while first_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    oldest_income = self.repository.get_oldest_income(account=self.account.alias)
                    if oldest_income is None:
                        # API will return inclusive, don't want to return the oldest record again
                        oldest_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                    else:
                        oldest_timestamp = oldest_income.timestamp
                        logger.warning(f'Synced trades before {oldest_timestamp}')

                    exchange_incomes = self.rest_manager.futures_income_history(
                        **{'limit': 1000, 'endTime': oldest_timestamp - 1})
                    logger.info(f"Length of older trades fetched up to {oldest_timestamp}: {len(exchange_incomes)}")
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income['asset']):
                            exchange_income['income'] = self.income_to_usdt(
                                float(exchange_income['income']),
                                int(exchange_income['time']),
                                exchange_income['asset'])
                            exchange_income['asset'] = "USDT"

                        income = Income(symbol=exchange_income['symbol'],
                                        asset=exchange_income['asset'],
                                        type=exchange_income['incomeType'],
                                        income=float(exchange_income['income']),
                                        timestamp=exchange_income['time'],
                                        transaction_id=exchange_income['tranId'])
                        incomes.append(income)
                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(exchange_incomes) < 1:
                        first_trade_reached = True

                # WARNING: don't use forward-walking only, because binance only returns max 7 days when using forward-walking
                # If this logic is ever changed, make sure that it's still able to retrieve all the account history
                newest_trade_reached = False
                while newest_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    newest_income = self.repository.get_newest_income(account=self.account.alias)
                    if newest_income is None:
                        # Binance started in September 2017, so no trade can be before that
                        newest_timestamp = int(
                            datetime.datetime.fromisoformat('2017-09-01 00:00:00+00:00').timestamp() * 1000)
                    else:
                        newest_timestamp = newest_income.timestamp
                        logger.warning(f'Synced newer trades since {newest_timestamp}')

                    exchange_incomes = self.rest_manager.futures_income_history(
                        **{'limit': 1000, 'startTime': newest_timestamp + 1})
                    logger.info(f"Length of newer trades fetched from {newest_timestamp}: {len(exchange_incomes)}")
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income['asset']):
                            exchange_income['income'] = self.income_to_usdt(
                                float(exchange_income['income']),
                                int(exchange_income['time']),
                                exchange_income['asset'])
                            exchange_income['asset'] = "USDT"

                        income = Income(symbol=exchange_income['symbol'],
                                        asset=exchange_income['asset'],
                                        type=exchange_income['incomeType'],
                                        income=float(exchange_income['income']),
                                        timestamp=exchange_income['time'],
                                        transaction_id=exchange_income['tranId'])
                        incomes.append(income)
                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(exchange_incomes) < 1:
                        newest_trade_reached = True

                logger.warning('Synced trades')
            except Exception as e:
                logger.error(f'{self.account.alias} Failed to process trades: {e}')

            time.sleep(60)

    def income_to_usdt(self, income: float, income_timestamp: int, asset: str) -> float:
        if is_asset_usd_or_derivative(asset):
            return income

        # Can't get the latest aggr_trades on just the endTime, so this is 'best effort'
        symbol = f"{asset}USDT"
        candles = self.rest_manager.futures_klines(symbol=symbol,
                                                   interval='1m',
                                                   startTime=int(income_timestamp) - 1000,
                                                   limit=1)

        close_price = candles[-1][4]
        income *= float(close_price)

        return income

    def sync_account(self):
        while True:
            try:
                account = self.rest_manager._request('get', self.rest_manager.FUTURES_URL + '/v2/account', True, data={})
                asset_balances = [AssetBalance(asset=asset['asset'],
                                               balance=float(
                                                   asset['walletBalance']),
                                               unrealizedProfit=float(
                                                   asset['unrealizedProfit'])
                                               ) for asset in account['assets']]

                usd_assets = [asset for asset in account['assets'] if asset['asset'] in ['BUSD', 'USDT', 'USDC']]
                total_wallet_balance = sum([float(asset['walletBalance']) for asset in usd_assets])
                total_upnl = sum([float(asset['unrealizedProfit']) for asset in usd_assets])

                logger.info(f'Wallet balance: {total_wallet_balance}, upnl: {total_upnl}')

                balance = Balance(totalBalance=total_wallet_balance,
                                  totalUnrealizedProfit=total_upnl,
                                  assets=asset_balances)
                self.repository.process_balances(balance, account=self.account.alias)

                positions = [Position(symbol=position['symbol'],
                                      entry_price=float(
                                          position['entryPrice']),
                                      position_size=float(
                                          position['positionAmt']),
                                      side=position['positionSide'],
                                      unrealizedProfit=float(
                                          position['unrealizedProfit']),
                                      initial_margin=float(position['initialMargin'])
                                      ) for position in account['positions'] if position['positionSide'] != 'BOTH']
                self.repository.process_positions(positions, account=self.account.alias)
                mark_prices = self.rest_manager.futures_mark_price()

                for position in positions:
                    if position.position_size != 0.0:
                        symbol = position.symbol
                        mark_price = [p for p in mark_prices if p['symbol'] == symbol][0]
                        tick = Tick(symbol=symbol,
                                    price=float(mark_price['markPrice']),
                                    qty=-1,
                                    timestamp=int(mark_price['time']))
                        self.repository.process_tick(tick, account=self.account.alias)
                        logger.debug(f'Synced recent trade price for {symbol}')
                # [self.add_to_ticker(position.symbol) for position in positions if position.position_size > 0.0]
                logger.warning('Synced account')
            except Exception as e:
                logger.error(f'{self.account.alias} Failed to process balance: {e}')
            time.sleep(20)


    def sync_open_orders(self):
        while True:
            orders = []
            try:
                open_orders = self.rest_manager.futures_get_open_orders()
                for open_order in open_orders:
                    order = Order()
                    order.symbol = open_order['symbol']
                    order.price = float(open_order['price'])
                    order.quantity = float(open_order['origQty'])
                    order.side = open_order['side']
                    order.position_side = open_order['positionSide']
                    order.type = open_order['type']
                    orders.append(order)
                self.repository.process_orders(orders, account=self.account.alias)
                logger.warning(f'Synced orders')

                headers = self.rest_manager.response.headers._store
                logger.info(f'API weight: {int(headers["x-mbx-used-weight-1m"][1])}')
            except Exception as e:
                logger.error(f'{self.account.alias} Failed to process open orders for symbol: {e}')

            time.sleep(30)

    def add_to_ticker(self, symbol: str):
        if symbol not in self.tick_symbols:
            symbol_trade_thread = threading.Thread(
                name=f'trade_thread_{symbol}', target=self.process_trades, args=(symbol,), daemon=True)
            symbol_trade_thread.start()

    def process_trades(self, symbol: str):
        if symbol in self.tick_symbols:
            logger.error(f'Already listening to ticks for {symbol}, not starting new processing!')
            return
        self.tick_symbols.append(symbol)

        # stream buffer is set to length 1, because we're only interested in the most recent tick
        self.ws_manager.create_stream(channels=['aggTrade'],
                                      markets=symbol,
                                      stream_buffer_name=f"trades_{symbol}",
                                      output="UnicornFy",
                                      stream_buffer_maxlen=1)
        logger.info(f"Trade stream started for {symbol}")
        while True:
            try:
                if self.ws_manager.is_manager_stopping():
                    logger.debug('Stopping trade-stream processing...')
                    break
                event = self.ws_manager.pop_stream_data_from_stream_buffer(
                    stream_buffer_name=f"trades_{symbol}")
                if event and 'event_type' in event and event['event_type'] == 'aggTrade':
                    logger.debug(event)
                    tick = Tick(symbol=event['symbol'],
                                price=float(event['price']),
                                qty=float(event['quantity']),
                                timestamp=int(event['trade_time']))
                    logger.debug(f"Processed tick for {tick.symbol}")
                    self.repository.process_tick(tick, account=self.account.alias)
            except Exception as e:
                logger.warning(f'Error processing tick: {e}')
            # Price update every 5 seconds is fast enough
            time.sleep(5)
        logger.warning('Stopped trade-stream processing')
