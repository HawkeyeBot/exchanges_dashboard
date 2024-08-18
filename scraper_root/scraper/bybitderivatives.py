import logging
import threading
import time
import datetime

from dateutil import parser
from typing import List, Dict

from pybit.exceptions import FailedRequestError
from pybit.unified_trading import HTTP

from scraper_root.scraper.data_classes import AssetBalance, Position, Tick, Balance, Income, Order, \
    Account
from scraper_root.scraper.persistence.repository import Repository
from scraper_root.scraper.utils import readable

logger = logging.getLogger()


def is_asset_usd_or_derivative(symbol: str):
    if 'usdt' in symbol.lower():
        return True
    if 'usd' in symbol.lower():
        return True
    if 'usdc' in symbol.lower():
        return True
    return False


class BybitDerivatives:
    def __init__(self, account: Account, symbols: List[str], repository: Repository, unified_account: bool):
        logger.info(f"Bybit initializing")
        self.account = account
        self.unified_account = unified_account
        self.alias = self.account.alias
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.repository = repository
        #        self.ws_manager = BybitWebsocket(wsURL="wss://stream-testnet.bybit.com/realtime_private",
        #            api_key=self.api_key, api_secret=self.secret)
        # bybit connection
        self.rest_manager2 = HTTP(testnet=False, api_key=self.api_key, api_secret=self.secret)

        # check if i am able to login
        test = self.rest_manager2.get_api_key_information()
        if test['retCode'] == 0:
            logger.info(f"{self.alias}: rest login succesfull")
        else:
            logger.error(f"{self.alias}: failed to login")
            logger.error(f"{self.alias}: exiting")
            raise SystemExit()

        # pull all USDT symbols and create a list.
        self.linearsymbols = []
        linearsymbolslist = self.rest_manager2.get_instruments_info(category='linear',
                                                                    limit=1000,
                                                                    status='Trading')
        try:
            for i in linearsymbolslist['result']['list']:
                if i['quoteCoin'] == 'USDT':
                    self.linearsymbols.append(i['symbol'])
        except Exception as e:
            logger.error(f'{self.alias}: Failed to pull linearsymbols: {e}')

        # globals
        #        global activesymbols
        self.activesymbols = []  # list
        self.asset_symbol: Dict[str, str] = {}

    def start(self):
        logger.info(f'{self.alias}: Starting Bybit Derivatives scraper')

        for symbol in self.symbols:
            symbol_trade_thread = threading.Thread(name=f'trade_thread_{symbol}', target=self.sync_current_price,
                                                   args=(symbol,), daemon=True)
            symbol_trade_thread.start()

        sync_balance_thread = threading.Thread(name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        sync_positions_thread = threading.Thread(name=f'sync_positions_thread', target=self.sync_positions, daemon=True)
        sync_positions_thread.start()

        sync_trades_thread = threading.Thread(name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

        sync_orders_thread = threading.Thread(name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
        sync_orders_thread.start()

    def sync_account(self):
        while True:
            try:
                accounttype = "UNIFIED" if self.unified_account is True else "CONTRACT"
                account = self.rest_manager2.get_wallet_balance(accountType=accounttype)
                assets = account['result']['list']
                balances = []
                total_usdt_balance = 0
                total_upnl = 0
                for asset in assets:
                    for coin in asset['coin']:
                        if coin['coin'] == 'USDT':
                            total_usdt_balance += float(coin['walletBalance'])
                            total_upnl += float(coin['unrealisedPnl'])
                        else:
                            balances.append(AssetBalance(asset=coin['coin'], balance=float(coin['walletBalance']), unrealizedProfit=float(coin['unrealisedPnl'])))

                # bybit has no total assets balance, assuming USDT
                balance = Balance(totalBalance=total_usdt_balance,
                                  totalUnrealizedProfit=total_upnl,
                                  assets=balances)
                self.repository.process_balances(balance=balance, account=self.alias)
                logger.warning(f'{self.alias}: Synced balance')
                time.sleep(100)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process balance: {e}')
                time.sleep(360)
                pass

    def sync_positions(self):
        while True:
            try:
                #                global activesymbols
                self.activesymbols = ["BTCUSDT"]
                positions = []
                exchange_position = self.rest_manager2.get_positions(category='linear', settleCoin="USDT")
                for x in exchange_position['result']['list']:
                    if float(x['size']) != 0:  # filter only items that have positions
                        if x['positionIdx'] == 1:  # recode buy / sell into long / short
                            side = "LONG"
                        else:
                            side = "SHORT"
                        self.activesymbols.append(x['symbol'])

                        positions.append(Position(symbol=x['symbol'],
                                                  entry_price=float(x['avgPrice']),
                                                  position_size=float(x['size']),
                                                  side=side,
                                                  # make it the same as binance data, bybit data is : item['side'],
                                                  unrealizedProfit=float(x['unrealisedPnl']),
                                                  initial_margin=0.0)  # TODO: float(x['position_margin'])
                                         )
                self.repository.process_positions(positions=positions, account=self.alias)
                logger.warning(f'{self.alias}: Synced positions')
                # logger.info(f'test: {self.activesymbols}')
                time.sleep(250)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process positions: {e}')
                time.sleep(360)

    def sync_open_orders(self):
        while True:
            orders = []
            if len(self.activesymbols) > 1:  # if activesymbols has more than 1 item do stuff
                for symbol in self.activesymbols:
                    try:  # when there a new symbols a pnl request fails with an error and scripts stops. so in a try and pass.
                        open_orders = self.rest_manager2.get_open_orders(category='linear', symbol=symbol, limit=50)  # queries open orders only by default
                        for item in open_orders["result"]['list']:
                            order = Order()
                            order.symbol = item['symbol']
                            order.price = float(item['price'])
                            order.quantity = float(item['qty'])
                            order.side = item['side'].upper()  # upper() to make it the same as binance
                            # bybit has no 'position side', assuming 'side'
                            #                                if item['side'] == "Buy":  # recode buy / sell into long / short
                            #                                    side = "SHORT"  # note: reversed. buy=short,sell = long
                            #                                else:
                            #                                    side = "LONG"
                            if item['side'] == "Buy":  # recode buy / sell into long / short
                                if item['reduceOnly']:
                                    side = "SHORT"
                                else:
                                    side = "LONG"
                            else:
                                if item['reduceOnly']:
                                    side = "LONG"
                                else:
                                    side = "SHORT"
                            order.position_side = side
                            order.type = item['orderType']
                            orders.append(order)
                    except:
                        logger.exception(f'{self.alias}: Failed to process orders')
                        time.sleep(30)
                logger.warning(f'{self.alias}: Synced orders')
                self.repository.process_orders(orders=orders, account=self.alias)
            time.sleep(120)  # pause after 1 complete run

    # #WS stream bybit; for future use, cannot limit ws stream
    #     def process_trades(self, symbol: str):
    #         subs = [
    #             "trade."[symbol]
    #             ]
    #         self.ws_trades = WebSocket(
    #             "wss://stream-testnet.bybit.com/realtime_public",
    #             subscriptions=subs
    #         )
    #         logger.info(f"Trade stream started")
    #         while True:
    #             if self.ws_trades.is_trades_stopping():
    #                  logger.debug('Stopping trade-stream processing...')
    #                  break
    # #             event = self.ws_manager.pop_stream_data_from_stream_buffer(stream_buffer_name=f"trades_{symbol}")
    # #             if event and 'event_type' in event and event['event_type'] == 'aggTrade':
    # #                 logger.debug(event)
    # #                 tick = Tick(symbol=event['symbol'],
    # #                             price=float(event['price']),
    # #                             qty=float(event['quantity']),
    # #                             timestamp=int(event['trade_time']))
    # #                 logger.debug(f"Processed tick for {tick.symbol}")
    # #                 self.repository.process_tick(tick)
    # #             # Price update every 5 seconds is fast enough
    # #             time.sleep(5)
    # #         logger.warning('Stopped trade-stream processing')
    #             data = self.ws_trades.fetch(subs[0])
    #             if data:
    #                 print(data)

    def sync_current_price(self, symbol: str):
        while True:
            # logger.info(f"Trade stream started")
            try:
                for i in self.activesymbols:
                    event = self.rest_manager2.get_public_trade_history(category="linear", symbol="{}".format(i), limit='1')
                    event1 = event['result']['list'][0]
                    tick = Tick(symbol=event1['symbol'],
                                price=float(event1['price']),
                                qty=float(event1['size']),
                                timestamp=int(event1['time']))
                    self.repository.process_tick(tick=tick, account=self.alias)
                logger.info(f"{self.alias}: Processed ticks")
                time.sleep(60)
            except Exception as e:
                logger.warning(f'{self.alias}: Failed to process ticks: {e}')
                time.sleep(120)
                pass

    def sync_trades(self):
        max_fetches_in_cycle = 3
        first_trade_reached = False
        one_day_ms = 24 * 60 * 60 * 1000
        while True:
            try:
                two_years_ago = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2*365))
                two_years_ago = int(two_years_ago.timestamp() * 1000)

                counter = 0
                while first_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    oldest_income = self.repository.get_oldest_income(account=self.account.alias)
                    if oldest_income is None:
                        # API will return inclusive, don't want to return the oldest record again
                        oldest_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                    else:
                        oldest_timestamp = oldest_income.timestamp
                        logger.warning(f'Synced trades before {readable(oldest_timestamp)}')

                    oldest_timestamp = max(oldest_timestamp, two_years_ago)

                    exchange_incomes = self.rest_manager2.get_closed_pnl(category="linear", limit='100', startTime=oldest_timestamp - one_day_ms, endTime=oldest_timestamp - 1)
                    logger.info(f"Length of older trades fetched up to {readable(oldest_timestamp)}: {len(exchange_incomes['result']['list'])}")
                    incomes = []

                    for exchange_income in exchange_incomes['result']['list']:
                        asset = self.get_asset(exchange_income['symbol'])
                        if not is_asset_usd_or_derivative(exchange_income['symbol']):
                            exchange_income['income'] = self.income_to_usdt(
                                float(exchange_income['income']),
                                int(exchange_income['updatedTime']),
                                asset)

                        income = Income(symbol=exchange_income['symbol'],
                                        asset="USDT",
                                        type='REALIZED_PNL',
                                        income=float(exchange_income['closedPnl']),
                                        timestamp=int(exchange_income['createdTime']),
                                        transaction_id=exchange_income['orderId'])
                        incomes.append(income)

                    while exchange_incomes['result']['nextPageCursor'] != '':
                        logger.info(f"{self.alias}: Retrieving orders for page cursor {exchange_incomes['result']['nextPageCursor']}'")
                        for exchange_income in exchange_incomes['result']['list']:
                            asset = self.get_asset(exchange_income['symbol'])
                            if not is_asset_usd_or_derivative(exchange_income['symbol']):
                                exchange_income['income'] = self.income_to_usdt(
                                    float(exchange_income['income']),
                                    int(exchange_income['time']),
                                    asset)

                            income = Income(symbol=exchange_income['symbol'],
                                            asset="USDT",
                                            type='REALIZED_PNL',
                                            income=float(exchange_income['closedPnl']),
                                            timestamp=int(exchange_income['createdTime']),
                                            transaction_id=exchange_income['orderId'])
                            incomes.append(income)

                        exchange_incomes = self.rest_manager2.get_closed_pnl(category="linear", limit='100', endTime=oldest_timestamp - 1,
                                                                             cursor=exchange_incomes['result']['nextPageCursor'])
                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(incomes) < 1:
                        first_trade_reached = True

                # WARNING: don't use forward-walking only, because binance only returns max 7 days when using forward-walking
                # If this logic is ever changed, make sure that it's still able to retrieve all the account history
                newest_trade_reached = False
                while newest_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    newest_income = self.repository.get_newest_income(account=self.account.alias)
                    if newest_income is None:
                        # only support trades after 2020, so no trade can be before that
                        newest_timestamp = int(datetime.datetime.fromisoformat('2020-01-01 00:00:00+00:00').timestamp() * 1000)
                    else:
                        newest_timestamp = newest_income.timestamp
                        logger.warning(f'Synced newer trades since {readable(newest_timestamp)}')

                    newest_timestamp = max(newest_timestamp, two_years_ago)

                    exchange_incomes = self.rest_manager2.get_closed_pnl(category="linear", limit='100', startTime=newest_timestamp + 1)
                    logger.info(f"Length of newer trades fetched from {readable(newest_timestamp)}: {len(exchange_incomes['result']['list'])}")
                    incomes = []
                    for exchange_income in exchange_incomes['result']['list']:
                        asset = self.get_asset(exchange_income['symbol'])
                        if not is_asset_usd_or_derivative(exchange_income['symbol']):
                            exchange_income['income'] = self.income_to_usdt(
                                float(exchange_income['income']),
                                int(exchange_income['updatedTime']),
                                asset)

                        income = Income(symbol=exchange_income['symbol'],
                                        asset="USDT",
                                        type='REALIZED_PNL',
                                        income=float(exchange_income['closedPnl']),
                                        timestamp=int(exchange_income['createdTime']),
                                        transaction_id=exchange_income['orderId'])
                        incomes.append(income)

                    while exchange_incomes['result']['nextPageCursor'] != '':
                        for exchange_income in exchange_incomes['result']['list']:
                            asset = self.get_asset(exchange_income['symbol'])
                            if not is_asset_usd_or_derivative(exchange_income['symbol']):
                                exchange_income['income'] = self.income_to_usdt(
                                    float(exchange_income['income']),
                                    int(exchange_income['updatedTime']),
                                    asset)

                            income = Income(symbol=exchange_income['symbol'],
                                            asset="USDT",
                                            type='REALIZED_PNL',
                                            income=float(exchange_income['closedPnl']),
                                            timestamp=int(exchange_income['createdTime']),
                                            transaction_id=exchange_income['orderId'])
                            incomes.append(income)

                        exchange_incomes = self.rest_manager2.get_closed_pnl(category="linear", limit='100', startTime=newest_timestamp + 1,
                                                                             cursor=exchange_incomes['result']['nextPageCursor'])
                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(incomes) < 1:
                        newest_trade_reached = True

                logger.warning('Synced trades')
            except Exception as e:
                logger.exception(f'{self.account.alias} Failed to process trades: {e}')

            time.sleep(60)

    def income_to_usdt(self, income: float, income_timestamp: int, asset: str) -> float:
        if is_asset_usd_or_derivative(asset):
            return income

        # Can't get the latest aggr_trades on just the endTime, so this is 'best effort'
        symbol = f"{asset}USDT"
        candles = self.rest_manager2.get_kline(category="linear",
                                               symbol=symbol,
                                               interval='1',
                                               start=int(income_timestamp) - 1000,
                                               limit=1)

        close_price = candles['result']['list'][-1][4]
        income *= float(close_price)

        return income

    def get_asset(self, symbol: str):
        if symbol not in self.asset_symbol:
            try:
                self.asset_symbol[symbol] = self.rest_manager2.get_instruments_info(category="linear", symbol=symbol)['result']['list'][0]['quoteCoin']
                return self.asset_symbol[symbol]
            except:
                logger.exception(f"Failed to retrieve quoteCoin for symbol {symbol}, falling back to USDT")
                return 'USDT'
