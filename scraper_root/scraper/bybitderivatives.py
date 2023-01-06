import datetime
import logging
import threading
import time
from typing import List

from pybit import HTTP

from scraper_root.scraper.data_classes import AssetBalance, Position, ScraperConfig, Tick, Balance, Income, Order, \
    Account
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


class BybitDerivatives:
    def __init__(self, account: Account, symbols: List[str], repository: Repository, exchange: str = "bybit"):
        logger.info(f"Bybit initializing")
        self.account = account
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.repository = repository
#        self.ws_manager = BybitWebsocket(wsURL="wss://stream-testnet.bybit.com/realtime_private", 
#            api_key=self.api_key, api_secret=self.secret)
        #bybit connection
        api_link = "https://api.bybit.com"
        if self.account.test_net:
            api_link = "https://api-testnet.bybit.com"
        self.rest_manager2 = HTTP(api_link, api_key=self.api_key, api_secret=self.secret)

        # check if i am able to login
        test = self.rest_manager2.api_key_info()
        testlist = ["OK", "ok", "Ok"]
        if test['ret_msg'] in testlist:
            logger.info(f"rest login succesfull")
        else:
            logger.error(f"failed to login")
            logger.error(f"exiting")
            raise SystemExit()

        # pull all USDT symbols and create a list.
        global linearsymbols
        linearsymbols = []
        linearsymbolslist = self.rest_manager2.query_symbol()
        try:
            for i in linearsymbolslist['result']:
                if i['quote_currency'] == 'USDT':
                    linearsymbols.append(i['alias'])
        except Exception as e:
            logger.error(f'Failed to pull linearsymbols: {e}')

        # globals
        global activesymbols
        activesymbols = []  # list

    def start(self):
        print('Starting Bybit Derivatives scraper')

        for symbol in self.symbols:
            symbol_trade_thread = threading.Thread(name=f'trade_thread_{symbol}', target=self.process_trades,
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

        sync_trades_thread.join()

    def sync_account(self):
        while True:
            try:
                account = self.rest_manager2.get_wallet_balance()
                assets = account['result']
                asset_balances = [AssetBalance(asset=asset,
                                               balance=float(assets[asset]['wallet_balance']),
                                               unrealizedProfit=float(assets[asset]['unrealised_pnl'])
                                               ) for asset in assets]

                # bybit has no total assets balance, assuming USDT
                balance = Balance(totalBalance=assets['USDT']['wallet_balance'],
                                  totalUnrealizedProfit=assets['USDT']['unrealised_pnl'],
                                  assets=asset_balances)
                self.repository.process_balances(balance=balance, account=self.account.alias)
                logger.warning('Synced balance')
                time.sleep(100)
            except Exception as e:
                logger.error(f'Failed to process balance: {e}')
                time.sleep(360)
                pass

    def sync_positions(self):
        while True:
            try:
                global activesymbols
                activesymbols = ["BTCUSDT"]
                positions = []
                for i in linearsymbols:
                    exchange_position = self.rest_manager2.my_position(symbol="{}".format(i))
                    for x in exchange_position['result']:
                        if x['position_value'] != 0:  # filter only items that have positions
                            if x['side'] == "Buy":  # recode buy / sell into long / short
                                side = "LONG"
                            else:
                                side = "SHORT"
                            activesymbols.append(x['symbol'])

                            positions.append(Position(symbol=x['symbol'],
                                                      entry_price=float(x['entry_price']),
                                                      position_size=float(x['size']),
                                                      side=side,
                                                      # make it the same as binance data, bybit data is : item['side'],
                                                      unrealizedProfit=float(x['unrealised_pnl']),
                                                      initial_margin=float(x['position_margin']))
                                             )
                self.repository.process_positions(positions=positions, account=self.account.alias)
                logger.warning('Synced positions')
                # logger.info(f'test: {activesymbols}')
                time.sleep(250)
            except Exception as e:
                logger.error(f'Failed to process positions: {e}')
                time.sleep(360)
                pass

    def sync_open_orders(self):
        while True:
            orders = []
            if len(activesymbols) > 1:  # if activesymbols has more than 1 item do stuff
                for i in activesymbols:
                    try:  # when there a new symbols a pnl request fails with an error and scripts stops. so in a try and pass.
                        open_orders = self.rest_manager2.get_active_order(symbol="{}".format(i), order_status="New")
                        if not open_orders['result']['data']:  # note: None = empty.
                            pass
                        else:
                            for item in open_orders["result"]['data']:
                                order = Order()
                                order.symbol = item['symbol']
                                order.price = float(item['price'])
                                order.quantity = float(item['qty'])
                                order.side = item['side'].upper()  # upper() to make it the same as binance
                                # bybit has no 'position side', assuming 'side'
                                if item['side'] == "Buy":  # recode buy / sell into long / short
                                    side = "SHORT"  # note: reversed. buy=short,sell = long
                                else:
                                    side = "LONG"
                                order.position_side = side
                                order.type = item['order_type']
                                orders.append(order)
                    except Exception as e:
                        logger.warning(f'Failed to process orders: {e}')
                        time.sleep(360)
                        pass
                logger.warning('Synced orders')
                self.repository.process_orders(orders=orders, account=self.account.alias)
            time.sleep(140)  # pause after 1 complete run

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

    def process_trades(self, symbol: str):
        while True:
            # logger.info(f"Trade stream started")
            if len(activesymbols) > 1:  # if activesymbols has more than 1 item do stuff
                try:
                    for i in activesymbols:
                        event = self.rest_manager2.public_trading_records(symbol="{}".format(i), limit='1')
                        event1 = event['result'][0]
                        tick = Tick(symbol=event1['symbol'],
                                    price=float(event1['price']),
                                    qty=float(event1['qty']),
                                    timestamp=int(event1['trade_time_ms']))
                        self.repository.process_tick(tick=tick, account=self.account.alias)
                    logger.info(f"Processed ticks")
                    time.sleep(130)
                except Exception as e:
                    logger.warning(f'Failed to process trades: {e}')
                    time.sleep(360)
                    pass

    def sync_trades(self):
        x = 0  # not pythonic but it works
        while True:
            if x == 0:
                # fill table on first run with 50 pages x 50 limit. TODO: full all-time history data and limit to one? page after inital fill
                for i in linearsymbols:
                    try:  # when there is a new symbol, pnl request fails with an error and scripts stops. so in a try and pass.
                        exchange_pnl = self.rest_manager2.closed_profit_and_loss(symbol="{}".format(i), limit='50')
                        #    pprint (exchange_pnl)
                        if not exchange_pnl['result']['data']:  # note: None = empty.
                            pass
                        else:
                            for page in range(1, 50):
                                exchange_pnl = self.rest_manager2.closed_profit_and_loss(symbol="{}".format(i),
                                                                                         limit='50',
                                                                                         page="{}".format(page))
                                # print (exchange_pnl["result"]['data'])
                                if not exchange_pnl['result']['data']:  # note: None = empty.
                                    pass
                                else:
                                    incomes = []
                                    for exchange_income in exchange_pnl["result"]['data']:
                                        timestamp2 = (exchange_income[
                                                          'created_at'] * 1000)  # *1000 needed for repository.py
                                        income = Income(symbol=exchange_income['symbol'],
                                                        asset='USDT',
                                                        type=exchange_income['exec_type'],
                                                        income=float(exchange_income['closed_pnl']),
                                                        # timestamp=exchange_income['created_at'],
                                                        timestamp=timestamp2,
                                                        transaction_id=exchange_income['order_id'])
                                        incomes.append(income)
                                    self.repository.process_incomes(incomes=incomes, account=self.account.alias)
                            time.sleep(5)  # pause to not overload the api limit
                    except Exception:
                        time.sleep(360)
                        pass
                x += 1
                logger.info('Synced initial trades')
            else:
                for i in linearsymbols:
                    try:  # when there is a new symbol, pnl request fails with an error and scripts stops. so in a try and pass.
                        exchange_pnl = self.rest_manager2.closed_profit_and_loss(symbol="{}".format(i), limit='50')
                        #    pprint (exchange_pnl)
                        if not exchange_pnl['result']['data']:  # note: None = empty.
                            pass
                        else:
                            incomes = []
                            for exchange_income in exchange_pnl["result"]['data']:
                                timestamp2 = (exchange_income['created_at'] * 1000)  # *1000 needed for repository.py
                                income = Income(symbol=exchange_income['symbol'],
                                                asset='USDT',
                                                type=exchange_income['exec_type'],
                                                income=float(exchange_income['closed_pnl']),
                                                # timestamp=exchange_income['created_at'],
                                                timestamp=timestamp2,
                                                transaction_id=exchange_income['order_id'])
                                incomes.append(income)
                            self.repository.process_incomes(incomes=incomes, account=self.account.alias)
                        time.sleep(5)  # pause to not overload the api limit
                    except Exception:
                        time.sleep(360)
                        pass
                logger.warning('Synced trades')
            time.sleep(120)
