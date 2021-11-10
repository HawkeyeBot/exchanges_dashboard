import bybit
from BybitWebsocket import BybitWebsocket

import datetime
import logging
import threading
import time
from typing import List
from pprint import pprint

from scraper_root.scraper.data_classes import AssetBalance, Position, ScraperConfig, Tick, Balance, Income, Order
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()

class BybitDerivatives:
    def __init__(self, config: ScraperConfig, repository: Repository, exchange: str = "bybit"):
        print('Bybit initializing')
        self.config = config
        self.api_key = self.config.api_key
        self.secret = self.config.api_secret
        self.repository = repository
        self.ws_manager = BybitWebsocket(wsURL="wss://stream-testnet.bybit.com/realtime_private", 
            api_key=self.api_key, api_secret=self.secret)
        self.rest_manager = bybit.bybit(test=False, api_key=self.api_key, api_secret=self.secret)

        # check if i am able to login
        test = self.rest_manager.APIkey.APIkey_info().result()
        if test[0]['ret_msg'] == 'ok':
            print('login succesfull')
        else:
            print('failed to login')
            print('exiting')
            raise SystemExit()
        
        #pull all USDT symbols and create a list.
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

#orders ----
#        for symbol in self.config.symbols:
#            symbol_trade_thread = threading.Thread(name=f'trade_thread_{symbol}', target=self.process_trades, args=(symbol,), daemon=True)
#            symbol_trade_thread.start()


        sync_balance_thread = threading.Thread(name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        sync_trades_thread = threading.Thread(name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

#openorders based on config file list - TODO
#        sync_orders_thread = threading.Thread(name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
#        sync_orders_thread.start()
#----

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

#TODO
#                positions = [Position(symbol=position['symbol'],
#                                      entry_price=float(position['entryPrice']),
#                                      position_size=float(position['positionAmt']),
#                                      side=position['positionSide'],
#                                      unrealizedProfit=float(position['unrealizedProfit'])
#                                      ) for position in account['positions'] if position['positionSide'] != 'BOTH']
#                self.repository.process_positions(positions)
                logger.warning('Synced account')
            except Exception as e:
                   logger.error(f'Failed to process balance: {e}')
#
            time.sleep(20)

#TODO - open orders based on list in config file.
#    def sync_open_orders(self):
#        while True:
#            try:
#                orders = {}
#                for symbol in self.config.symbols:
#                    open_orders = self.rest_manager.futures_get_open_orders(**{'symbol': symbol})
#                    orders[symbol] = []
#                    for open_order in open_orders:
#                        order = Order()
#                        order.symbol = open_order['symbol']
#                        order.price = float(open_order['price'])
#                        order.quantity = float(open_order['origQty'])
#                        order.side = open_order['side']
#                        order.position_side = open_order['positionSide']
#                        order.type = open_order['type']
#                        orders[symbol].append(order)
#                self.repository.process_orders(orders)
#                logger.warning('Synced orders')
#            except Exception as e:
#                logger.error(f'Failed to process open orders: {e}')
#
#            time.sleep(20)


#TODO - WS stream; oders obv symbols
#    def process_trades(self, symbol: str):
#        # stream buffer is set to length 1, because we're only interested in the most recent tick
#        self.ws_manager.create_stream(channels=['aggTrade'],
#                                      markets=symbol,
#                                      stream_buffer_name=f"trades_{symbol}",
#                                      output="UnicornFy",
#                                      stream_buffer_maxlen=1)
#        logger.info(f"Trade stream started")
#        while True:
#            if self.ws_manager.is_manager_stopping():
#                logger.debug('Stopping trade-stream processing...')
#                break
#            event = self.ws_manager.pop_stream_data_from_stream_buffer(stream_buffer_name=f"trades_{symbol}")
#            if event and 'event_type' in event and event['event_type'] == 'aggTrade':
#                logger.debug(event)
#                tick = Tick(symbol=event['symbol'],
#                            price=float(event['price']),
#                            qty=float(event['quantity']),
#                            timestamp=int(event['trade_time']))
#                logger.debug(f"Processed tick for {tick.symbol}")
#                self.repository.process_tick(tick)
#            # Price update every 5 seconds is fast enough
#            time.sleep(5)
#        logger.warning('Stopped trade-stream processing')


    def sync_trades(self):
        first_trade_reached = False
        print('start sync trades')
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
                        logger.info(f"Length of older trades fetched up to {oldest_timestamp}: {len(exchange_incomes)}")
                        if not exchange_incomes[0]['result']['data']: #note: None = empty. 
                            pass
                        else:
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
                            pprint(income) #debug
                            if len(exchange_incomes) < 1:
                                first_trade_reached = True

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
                    if not exchange_incomes[0]['result']['data']: #note: None = empty. 
                        pprint('none')
                    else:
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

                logger.warning('Synced trades')
            except Exception as e:
                logger.error(f'Failed to process sync trades: {e}')

    time.sleep(5)
