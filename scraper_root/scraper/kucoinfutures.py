import datetime
import logging
import logging
import threading
import time
from typing import List

from kucoin_futures.client import Market, User, Trade

from scraper_root.scraper.data_classes import AssetBalance, Position, Tick, Balance, \
    Order, Account, Income
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


def is_asset_usd_or_derivative(asset: str):
    return asset.lower() in ["usdt", "busd", "usd", "usdc"]


class KucoinFutures:
    def __init__(self, account: Account, symbols: List[str], repository: Repository):
        logger.info('Kucoin initialized')
        self.account = account
        self.symbols = ['XBTUSDTM']
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.passphrase = self.account.api_passphrase
        self.repository = repository
        self.market = Market()
        self.user = User(key=self.api_key, secret=self.secret, passphrase=self.passphrase)
        self.trade = Trade(key=self.api_key, secret=self.secret, passphrase=self.passphrase)

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

                    exchange_incomes = self.user.get_transaction_history(type='RealisedPNL', endAt=oldest_timestamp - 1, maxCount=1000)['dataList']
                    logger.info(f"Length of older trades fetched up to {oldest_timestamp}: {len(exchange_incomes)}")
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income['currency']):
                            exchange_income['amount'] = self.income_to_usdt(
                                float(exchange_income['amount']),
                                int(exchange_income['time']),
                                exchange_income['currency'])
                            exchange_income['currency'] = "USDT"

                        income = Income(symbol=exchange_income['remark'],
                                        asset=exchange_income['currency'],
                                        type=exchange_income['type'],
                                        income=float(exchange_income['amount']),
                                        timestamp=exchange_income['time'],
                                        transaction_id=exchange_income['offset'])
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

                    exchange_incomes = self.user.get_transaction_history(type='RealisedPNL', startAt=newest_timestamp + 1, maxCount=1000)['dataList']
                    logger.info(f"Length of newer trades fetched from {newest_timestamp}: {len(exchange_incomes)}")
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income['currency']):
                            exchange_income['amount'] = self.income_to_usdt(
                                float(exchange_income['amount']),
                                int(exchange_income['time']),
                                exchange_income['currency'])
                            exchange_income['currency'] = "USDT"

                        income = Income(symbol=exchange_income['remark'],
                                        asset=exchange_income['currency'],
                                        type=exchange_income['type'],
                                        income=float(exchange_income['amount']),
                                        timestamp=exchange_income['time'],
                                        transaction_id=exchange_income['offset'])
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
        start_time = int(income_timestamp) - 1000
        candles = self.market.get_kline_data(symbol=symbol, granularity=1, begin_t=start_time, end_t=start_time + 61_000)
        close_price = candles[-1][4]
        income *= float(close_price)

        return income

    def sync_account(self):
        while True:
            try:
                account = self.user.get_account_overview(currency='USDT')
                usd_assets = [AssetBalance(asset=account['currency'],
                                           balance=float(account['marginBalance']),
                                           unrealizedProfit=float(account['unrealisedPNL']))]

                total_wallet_balance = sum([asset.balance for asset in usd_assets])
                total_upnl = sum([asset.unrealizedProfit for asset in usd_assets])

                logger.info(f'Wallet balance: {total_wallet_balance}, upnl: {total_upnl}')

                balance = Balance(totalBalance=total_wallet_balance,
                                  totalUnrealizedProfit=total_upnl,
                                  assets=usd_assets)
                self.repository.process_balances(balance, account=self.account.alias)

                positions = []
                positions_response = self.trade.get_all_position()
                for position in positions_response:
                    quantity = float(position['currentQty'])
                    position_side = 'LONG'
                    if quantity < 0:
                        position_side = 'SHORT'
                    positions.append(Position(symbol=position['symbol'],
                                              entry_price=float(position['avgEntryPrice']),
                                              position_size=quantity,
                                              side=position_side,
                                              unrealizedProfit=float(position['unrealisedPnl']),
                                              initial_margin=float(position['posMargin'])
                                              ))
                self.repository.process_positions(positions, account=self.account.alias)

                for position_symbol in [position.symbol for position in positions]:
                    mark_price = self.market.get_current_mark_price(symbol=position_symbol)
                    logger.debug(mark_price)
                    tick = Tick(symbol=mark_price['symbol'],
                                price=float(mark_price['value']),
                                qty=float('0'),
                                timestamp=int(mark_price['timePoint']))
                    logger.debug(f"Processed tick for {tick.symbol}")
                    self.repository.process_tick(tick, account=self.account.alias)
                # [self.add_to_ticker(position.symbol) for position in positions if position.position_size > 0.0]
                logger.warning('Synced account')
            except Exception as e:
                logger.error(f'{self.account.alias} Failed to process balance: {e}')
            time.sleep(20)

    def sync_open_orders(self):
        while True:
            orders = []
            try:
                open_orders = self.trade.get_order_list(pageSize=1000, status='active')
                contract_list = self.market.get_contracts_list()
                price_per_symbol = {}
                for contract in contract_list:
                    price_per_symbol[contract['symbol']] = contract['lastTradePrice']
                # go through all pages
                for open_order in open_orders['items']:
                    order = Order()
                    order.symbol = open_order['symbol']
                    order.price = float(open_order['price'])
                    order.quantity = float(open_order['size'])
                    order.side = str(open_order['side']).upper()
                    current_price = price_per_symbol[order.symbol]
                    if order.price < current_price and order.side == 'BUY':
                        order.position_side = 'LONG'
                    elif order.price < current_price and order.side == 'SELL':
                        order.position_side = 'SHORT'
                    elif order.price > current_price and order.side == 'BUY':
                        order.position_side = 'SHORT'
                    elif order.price > current_price and order.side == 'SELL':
                        order.position_side = 'LONG'
                    else:
                        logger.error(f'{self.account.alias}: Failed to identify positionside for order {open_order}')

                    order.type = open_order['type']
                    orders.append(order)
                self.repository.process_orders(orders, account=self.account.alias)
                logger.warning(f'Synced orders')
            except:
                logger.error(f'{self.account.alias} Failed to process open orders')

            time.sleep(30)

    # def add_to_ticker(self, symbol: str):
    #     if symbol not in self.tick_symbols:
    #         symbol_trade_thread = threading.Thread(
    #             name=f'trade_thread_{symbol}', target=self.process_trades, args=(symbol,), daemon=True)
    #         symbol_trade_thread.start()

    def process_trades(self, symbol: str):
        if symbol in self.tick_symbols:
            logger.error(f'Already listening to ticks for {symbol}, not starting new processing!')
            return
        self.tick_symbols.append(symbol)

        logger.info(f"Trade stream started for {symbol}")
        while True:
            try:
                mark_price = self.market.get_current_mark_price(symbol=symbol)
                logger.debug(mark_price)
                tick_symbol = 'BTCUSDT' if symbol == 'XBTUSDTM' else mark_price['symbol']
                tick = Tick(symbol=tick_symbol,
                            price=float(mark_price['value']),
                            qty=float('0'),
                            timestamp=int(mark_price['timePoint']))
                logger.debug(f"Processed tick for {tick.symbol}")
                self.repository.process_tick(tick, account=self.account.alias)
            except Exception as e:
                logger.warning(f'Error processing tick: {e}')
            # Price update every 5 seconds is fast enough
            time.sleep(5)
