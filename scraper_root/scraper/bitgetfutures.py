import logging
import logging
import threading
import time
from typing import List

from pybitget import Client
from pybitget import utils

from scraper_root.scraper.data_classes import AssetBalance, Position, Tick, Balance, Income, Order, \
    Account
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


class BitgetFutures:
    def __init__(self, account: Account, symbols: List[str], repository: Repository, exchange: str = "bitget"):
        logger.info(f"Bitget initializing")
        self.account = account
        self.alias = self.account.alias
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.passphrase = self.account.api_passphrase
        self.repository = repository
        # bitget connection
        self.rest_manager_bitget = Client(self.api_key, self.secret, passphrase=self.passphrase)

        # pull all USDT symbols and create a list.
        self.linearsymbols = []
        linearsymbolslist = self.rest_manager_bitget.mix_get_symbols_info(productType='UMCBL')
        try:
            for i in linearsymbolslist['data']:
                if i['quoteCoin'] == 'USDT':
                    self.linearsymbols.append(i['symbol'][:-6])
        except Exception as e:
            logger.error(f'{self.alias}: Failed to pull linearsymbols: {e}')

        self.activesymbols = []  # list

    def start(self):
        logger.info(f'{self.alias}: Starting Bitget Futures scraper')

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

    def sync_account(self):
        while True:
            try:
                account = self.rest_manager_bitget.mix_get_accounts(productType='UMCBL')
                assets = account['data']
                asset_balances = [AssetBalance(asset=asset['marginCoin'], balance=float(asset['available']), unrealizedProfit=float(asset['unrealizedPL'])) for asset in assets]

                # bitget has no total assets balance, assuming USDT
                for asset in assets:
                    if asset['marginCoin'] == 'USDT':
                        balance = Balance(totalBalance=asset['available'], totalUnrealizedProfit=asset['unrealizedPL'], assets=asset_balances)
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
                for i in self.linearsymbols:
                    exchange_position = self.rest_manager_bitget.mix_get_single_position(symbol="{}_UMCBL".format(i),marginCoin='USDT')
                    for x in exchange_position['data']:
                        if x['total'] != '0':  # filter only items that have positions
                            if x['holdSide'] == "long":  # recode long / short into LONG / SHORT
                                side = "LONG"
                            else:
                                side = "SHORT"
                            self.activesymbols.append(x['symbol'][:-6])

                            positions.append(Position(symbol=x['symbol'][:-6],
                                                      entry_price=float(x['averageOpenPrice']),
                                                      position_size=float(x['total']),
                                                      side=side,
                                                      # make it the same as binance data, bitget data is : item['holdside'],
                                                      unrealizedProfit=float(x['unrealizedPL']),
                                                      initial_margin=float(x['margin']))
                                             )
                self.repository.process_positions(positions=positions, account=self.alias)
                logger.warning(f'{self.alias}: Synced positions')
                # logger.info(f'test: {self.activesymbols}')
                time.sleep(250)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process positions: {e}')
                time.sleep(360)
                pass

    def sync_open_orders(self):
        while True:
            orders = []
            if len(self.activesymbols) > 1:  # if activesymbols has more than 1 item do stuff
                for i in self.activesymbols:
                    try:  # when there a new symbols a pnl request fails with an error and scripts stops. so in a try and pass.
                        open_orders = self.rest_manager_bitget.mix_get_open_order(symbol="{}_UMCBL".format(i))
                        if not open_orders['data']:  # note: None = empty.
                            pass
                        else:
                            for item in open_orders["data"]:
                                order = Order()
                                order.symbol = item['symbol'][:-6]
                                order.price = float(item['price'])
                                order.quantity = float(item['size'])
                                if item['side'].startswith('open'):  # recode open / close into BUY / SELL
                                    side = "BUY"
                                else:
                                    side = "SELL"
                                order.side = side
                                order.position_side = item['posSide'].upper()  # upper() to make it the same as binance
                                order.type = item['orderType']
                                orders.append(order)
                    except Exception as e:
                        logger.warning(f'{self.alias}: Failed to process orders: {e}')
                        time.sleep(360)
                        pass
                logger.warning(f'{self.alias}: Synced orders')
                self.repository.process_orders(orders=orders, account=self.alias)
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
            if len(self.activesymbols) > 1:  # if activesymbols has more than 1 item do stuff
                try:
                    for i in self.activesymbols:
                        event = self.rest_manager_bitget.mix_get_fills(symbol="{}_UMCBL".format(i), limit='1')
                        event1 = event['data'][0]
                        tick = Tick(symbol=event1['symbol'][:-6],
                                    price=float(event1['price']),
                                    qty=float(event1['size']),
                                    timestamp=int(event1['timestamp']))
                        self.repository.process_tick(tick=tick, account=self.alias)
                    logger.info(f"{self.alias}: Processed ticks")
                    time.sleep(130)
                except Exception as e:
                    logger.warning(f'{self.alias}: Failed to process trades: {e}')
                    time.sleep(360)
                    pass

    def sync_trades(self):
        lastEnd = ''
        startT = '0'
        while True:
            endT = utils.get_timestamp()
            try:  # bitget returns all income, the symbol spcified dosn't matter
                exchange_pnl = self.rest_manager_bitget.mix_get_accountBill(symbol='BTCUSDT_UMCBL', marginCoin='USDT',startTime=startT,endTime=endT, pageSize='100')
                if not exchange_pnl['data']['result']:  # note: None = empty.
                    pass
                else:
                    while True:
                        incomes = []
                        for exchange_income in exchange_pnl["data"]['result']:
                            if exchange_income['symbol']:
                                if exchange_income['business'].startswith('close'):
                                    income_type = 'REALIZED_PNL'
                                else:
                                    income_type = exchange_income['business']
                                income = Income(symbol=exchange_income['symbol'][:-6],
                                                asset='USDT',
                                                type=income_type,
                                                income=float(exchange_income['amount'])+float(exchange_income['fee']),
                                                timestamp=int(int(exchange_income['cTime'])/1000)*1000,
                                                transaction_id=exchange_income['id'])
                                incomes.append(income)
                        self.repository.process_incomes(incomes=incomes, account=self.alias)
                        time.sleep(5)  # pause to not overload the api limit
                        if not exchange_pnl['data']['nextFlag']:
                            startT = endT
                            break
                        lastEnd = exchange_pnl['data']['lastEndId']
                        exchange_pnl = self.rest_manager_bitget.mix_get_accountBill(symbol='BTCUSDT_UMCBL', marginCoin='USDT',startTime=startT,endTime=endT, pageSize='100', lastEndId=lastEnd)
            except Exception:
                time.sleep(360)
                pass
            logger.info(f'{self.alias}: Synced trades')
            time.sleep(120)
