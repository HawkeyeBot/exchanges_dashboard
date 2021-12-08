import bybit

import datetime
import logging
import threading
import time

from scraper_root.scraper.data_classes import AssetBalance, Position, ScraperConfig, Tick, Balance, Income, Order
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()

class BybitDerivatives:
    def __init__(self, config: ScraperConfig, repository: Repository, exchange: str = "bybit"):
        logger.info(f"Bybit initializing")
        self.config = config
        self.api_key = self.config.api_key
        self.secret = self.config.api_secret
        self.repository = repository
#        self.ws_manager = BybitWebsocket(wsURL="wss://stream-testnet.bybit.com/realtime_private", 
#            api_key=self.api_key, api_secret=self.secret)
        self.rest_manager = bybit.bybit(test=False, api_key=self.api_key, api_secret=self.secret)

        # check if i am able to login
        test = self.rest_manager.APIkey.APIkey_info().result()
        if test[0]['ret_msg'] == 'ok':
            logger.info(f"rest login succesfull")
        else:
            logger.error(f"failed to login")
            logger.error(f"exiting")
            raise SystemExit()
        
        #pull all USDT symbols and create a list. TODO: update once every x.time
        global linearsymbols
        linearsymbols = []
        linearsymbolslist = self.rest_manager.Symbol.Symbol_get().result()
        try:
            for i in linearsymbolslist[0]['result']:
                if i['quote_currency'] == 'USDT':
                    linearsymbols.append(i['alias'])
        except Exception as e:
            logger.error(f'Failed to pull linearsymbols: {e}')

    def start(self):
        print('Starting Bybit Derivatives scraper')

        for symbol in self.config.symbols:
            symbol_trade_thread = threading.Thread(name=f'trade_thread_{symbol}', target=self.process_trades, args=(symbol,), daemon=True)
            symbol_trade_thread.start()

        sync_balance_thread = threading.Thread(name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        sync_trades_thread = threading.Thread(name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

        sync_orders_thread = threading.Thread(name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
        sync_orders_thread.start()

        sync_trades_thread.join()

    def sync_account(self):
        while True:
            try:
                account = self.rest_manager.Wallet.Wallet_getBalance().result()
                assets = account[0]['result']
                asset_balances = [AssetBalance(asset=asset,
                                            balance=float(assets[asset]['wallet_balance']),
                                            unrealizedProfit=float(assets[asset]['unrealised_pnl'])
                                            ) for asset in assets]
 
                #bybit has no total assets balance, assuming USDT
                balance = Balance(totalBalance=assets['USDT']['wallet_balance'],
                                  totalUnrealizedProfit=assets['USDT']['unrealised_pnl'],
                                  assets=asset_balances)
                self.repository.process_balances(balance)

                for i in linearsymbols:
                    exchange_position = self.rest_manager.LinearPositions.LinearPositions_myPosition(symbol="{}".format(i)).result()
                    if exchange_position[0]['result'][0]['position_value'] != 0: #note: None = empty. 
                        positions = [Position(symbol=position['symbol'],
                                            entry_price=float(position['entry_price']),
                                            position_size=float(position['position_value']),
                                            side=position['side'],
                                            unrealizedProfit=float(position['unrealised_pnl']),
                                            initial_margin=float(position['position_margin'])
                                            ) for position in exchange_position[0]['result']] # if position['positionSide'] != 'BOTH']
                        self.repository.process_positions(positions)
                    time.sleep(2)
                logger.warning('Synced account')
            except Exception as e:
                   logger.error(f'Failed to process balance: {e}')

            time.sleep(20)

    def sync_open_orders(self):
        while True:
            try:
                orders = {}
                for symbol in self.config.symbols:
                    open_orders = self.rest_manager.LinearOrder.LinearOrder_query(symbol="{}".format(symbol)).result()
                    orders = []
                    for open_order in open_orders[0]['result']:
                        order = Order()
                        order.symbol = open_order['symbol']
                        order.price = float(open_order['price'])
                        order.quantity = float(open_order['qty'])
                        order.side = open_order['side']
                        #bybit has no position side, assuming side
                        order.position_side = open_order['side']
                        order.type = open_order['order_type']
                        orders.append(order)
                self.repository.process_orders(orders)
                logger.warning('Synced orders')
            except Exception as e:
                logger.error(f'Failed to process open orders: {e}')

            time.sleep(20)


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
        event = self.rest_manager.LinearMarket.LinearMarket_trading(symbol="{}".format(symbol), limit="1").result()
        logger.info(f"Trade stream started")
        while True:
            event1 = event[0]['result']          
            tick = Tick(symbol=event1[0]['symbol'],
                            price=float(event1[0]['price']),
                            qty=float(event1[0]['qty']),
                            timestamp=int(event1[0]['trade_time_ms']))
            logger.debug(f"Processed tick for {tick.symbol}")
            self.repository.process_tick(tick)
             # Price update every 30 seconds is fast enough
            time.sleep(30)

    def sync_trades(self):
        first_trade_reached = False
        while True:
            try:
                counter = 0
                while first_trade_reached is False and counter < 3:
                    counter += 1
                    oldest_income = self.repository.get_oldest_income()
                    if oldest_income is None:
                        # API will return inclusive, don't want to return the oldest record again
                        oldest_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                    else:
                        oldest_timestamp = oldest_income.timestamp
                        logger.warning(f'Synced trades before {oldest_timestamp}')
                    for i in linearsymbols:
                        exchange_incomes = self.rest_manager.LinearPositions.LinearPositions_closePnlRecords(symbol="{}".format(i), limit="50", end_time=(oldest_timestamp - 1)).result()
                        if not exchange_incomes[0]['result']['data']: #note: None = empty. 
                            pass
                        else:
                            incomes = []
                            for exchange_income in exchange_incomes[0]['result']['data']:
                                timestamp2=(exchange_income['created_at']*1000) #needed for repository.py
                                income = Income(symbol=exchange_income['symbol'],
                                                asset='USDT',
                                                type=exchange_income['exec_type'],
                                                income=float(exchange_income['closed_pnl']),
                                                #timestamp=exchange_income['created_at'],
                                                timestamp=timestamp2,
                                                transaction_id=exchange_income['order_id'])
                                incomes.append(income)
                            self.repository.process_incomes(incomes)
                            if len(exchange_incomes) < 1:
                                first_trade_reached = True
                        time.sleep(2)
                    logger.info(f"Length of older trades fetched up to {oldest_timestamp}: {len(exchange_incomes)}")

                # WARNING: don't use forward-walking only, because binance only returns max 7 days when using forward-walking
                # If this logic is ever changed, make sure that it's still able to retrieve all the account history
                newest_trade_reached = False
                while newest_trade_reached is False and counter < 3:
                    counter += 1
                    newest_income = self.repository.get_newest_income()
                    if newest_income is None:
                        # Start from 2021-01-01
                        newest_timestamp = int(datetime.datetime.fromisoformat('2021-01-01 00:00:00+00:00').timestamp() * 1000)
                    else:
                        newest_timestamp = newest_income.timestamp
                        logger.warning(f'Synced newer trades since {newest_timestamp}')

                    for i in linearsymbols:
                        exchange_incomes = self.rest_manager.LinearPositions.LinearPositions_closePnlRecords(symbol="{}".format(i), limit="50", start_time=(newest_timestamp + 1)).result()
                        logger.info(f"Length of newer trades fetched from {newest_timestamp}: {len(exchange_incomes)}")
                        if exchange_incomes[0]['result']['data'] != 0: #note: None = empty. 
                            incomes = []
                            for exchange_income in exchange_incomes[0]['result']['data']:
                                income = Income(symbol=exchange_income['symbol'],
                                                asset='USDT',
                                                type=exchange_income['exec_type'],
                                                income=float(exchange_income['closed_pnl']),
                                                timestamp=exchange_income['created_at'],
                                                transaction_id=exchange_income['order_id'])
                            incomes.append(income)
                            self.repository.process_incomes(incomes)
                            if len(exchange_incomes) < 1:
                                newest_trade_reached = True
                        time.sleep(2)

                logger.warning('Synced trades')
            except Exception as e:
                logger.error(f'Failed to process sync trades: {e}')

            time.sleep(60) #60
