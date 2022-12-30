import datetime
import logging
import threading
import time
from typing import List

from unicorn_binance_rest_api import BinanceRestApiManager
from unicorn_binance_websocket_api import BinanceWebSocketApiManager

from scraper_root.scraper.data_classes import AssetBalance, Position, ScraperConfig, Tick, Balance, \
    Income, Order, Trade, Account
from scraper_root.scraper.persistence.orm_classes import TradeEntity
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


class BinanceSpot:
    def __init__(self, account: Account, symbols: List[str], repository: Repository, exchange: str = "binance.com"):
        print('Binance spot initialized')
        self.account = account
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.repository = repository
        self.ws_manager = BinanceWebSocketApiManager(exchange=exchange, throw_exception_if_unrepairable=True,
                                                     warn_on_update=False)

        self.rest_manager = BinanceRestApiManager(self.api_key, api_secret=self.secret)
        self.exchange_information = None
        self.tick_symbols = []

    def start(self):
        print('Starting binance spot scraper')

        self.exchange_information = self.rest_manager.get_exchange_info()
        sorted_symbols = [s for s in self.exchange_information['symbols'] if
                          s['status'] == 'TRADING' and s['quoteAsset'] in ['BTC', 'USDT', 'BUSD', 'USDC', 'USDP']]
        sorted_symbols.extend([s for s in self.exchange_information['symbols'] if s not in sorted_symbols])
        self.exchange_information['symbols'] = sorted_symbols
        symbol_search_thread = threading.Thread(name=f'userdata_thread',
                                                target=self.find_new_traded_symbols,
                                                daemon=True)
        symbol_search_thread.start()

        # userdata_thread = threading.Thread(name=f'userdata_thread', target=self.process_userdata, daemon=True)
        # userdata_thread.start()

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

    def find_new_traded_symbols(self):
        while True:
            try:
                counter = 0
                for item in self.exchange_information['symbols']:
                    if item['status'] != 'TRADING':
                        continue  # for performance reasons
                    symbol = item['symbol']
                    if symbol not in self.repository.get_symbol_checks(account=self.account.alias):
                        if not self.repository.is_symbol_traded(symbol, account=self.account.alias) and counter < 3:
                            trades = self.rest_manager.get_my_trades(**{'limit': 1, 'symbol': symbol})
                            counter += 1
                            self.repository.process_symbol_checked(symbol, account=self.account.alias)
                            if len(trades) > 0:
                                logger.info(f'Trades found for {symbol}, adding to sync list')
                                self.repository.process_traded_symbol(symbol, account=self.account.alias)
            except Exception as e:
                logger.error(f'Failed to verify unchecked symbols: {e}')

            logger.info('Updated new traded symbols')

            # TODO: once in a while the checked symbols that are not in the DB should be checked
            time.sleep(20)

    def get_asset(self, symbol: str) -> str:
        symbol_informations = self.exchange_information['symbols']
        for symbol_information in symbol_informations:
            if symbol_information['symbol'] == symbol:
                return symbol_information['baseAsset']
        raise Exception(f'No asset found for symbol {symbol}')

    def get_quote_asset(self, symbol: str) -> str:
        symbol_informations = self.exchange_information['symbols']
        for symbol_information in symbol_informations:
            if symbol_information['symbol'] == symbol:
                return symbol_information['quoteAsset']
        raise Exception(f'No asset found for symbol {symbol}')

    def sync_trades(self):
        first_trade_reached = {}  # key: symbol, value: bool
        max_downloads = 10
        while True:
            try:
                iteration_symbols = []
                counter = 0
                while counter < max_downloads:
                    # TODO: sync symbol of open position first if it was more than 5 minutes ago
                    symbol = self.repository.get_next_traded_symbol(account=self.account.alias)
                    logger.warning(f'Updating trades for {symbol}')
                    if symbol is not None:
                        self.repository.update_trades_last_downloaded(symbol=symbol, account=self.account.alias)
                    if symbol is None or symbol in iteration_symbols:
                        counter += 1
                        continue
                    iteration_symbols.append(symbol)
                    if symbol not in first_trade_reached:
                        first_trade_reached[symbol] = False
                    while first_trade_reached[symbol] is False and counter < max_downloads:
                        counter += 1
                        oldest_trade = self.repository.get_oldest_trade(symbol=symbol, account=self.account.alias)
                        if oldest_trade is None:
                            # API will return inclusive, don't want to return the oldest record again
                            oldest_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                        else:
                            oldest_timestamp = oldest_trade.timestamp
                            logger.warning(f'Synced trades before {oldest_timestamp} for {symbol}')

                        exchange_trades = self.rest_manager.get_my_trades(**{'symbol': symbol, 'limit': 1000,
                                                                             'endTime': oldest_timestamp - 1})
                        logger.info(
                            f"Length of older trades fetched up to {oldest_timestamp}: {len(exchange_trades)} for {symbol}")
                        trades = []
                        for exchange_trade in exchange_trades:
                            trade = Trade(symbol=exchange_trade['symbol'],
                                          asset=self.get_asset(exchange_trade['symbol']),
                                          order_id=exchange_trade['orderId'],
                                          quantity=exchange_trade['qty'],
                                          price=exchange_trade['price'],
                                          type='REALIZED_PNL',
                                          side='BUY' if exchange_trade['isBuyer'] is True else 'SELL',
                                          timestamp=int(exchange_trade['time']))
                            trades.append(trade)
                        self.repository.process_trades(trades=trades, account=self.account.alias)
                        if len(exchange_trades) < 1:
                            first_trade_reached[symbol] = True

                    # WARNING: don't use forward-walking only, because binance only returns max 7 days when using forward-walking
                    # If this logic is ever changed, make sure that it's still able to retrieve all the account history
                    newest_trade_reached = False
                    while newest_trade_reached is False and counter < max_downloads:
                        counter += 1
                        newest_trade = self.repository.get_newest_trade(symbol=symbol, account=self.account.alias)
                        if newest_trade is None:
                            # Binance started in September 2017, so no trade can be before that
                            # newest_timestamp = int(datetime.datetime.fromisoformat('2017-09-01 00:00:00+00:00').timestamp() * 1000)
                            newest_order_id = 0
                        else:
                            # newest_timestamp = newest_trade.timestamp
                            newest_order_id = newest_trade.order_id
                            # logger.warning(f'Synced newer trades since {newest_timestamp}')
                            logger.warning(f'Synced newer trades since {newest_order_id}')

                        exchange_trades = self.rest_manager.get_my_trades(**{'symbol': symbol,
                                                                             # 'limit': 1000,
                                                                             'orderId': newest_order_id + 1})
                        # 'startTime': newest_timestamp + 1})
                        logger.info(
                            f"Length of newer trades fetched from id {newest_order_id}: {len(exchange_trades)} for {symbol}")
                        trades = []
                        for exchange_trade in exchange_trades:
                            trade = Trade(symbol=exchange_trade['symbol'],
                                          asset=self.get_asset(exchange_trade['symbol']),
                                          order_id=exchange_trade['orderId'],
                                          quantity=exchange_trade['qty'],
                                          price=exchange_trade['price'],
                                          type='REALIZED_PNL',
                                          side='BUY' if exchange_trade['isBuyer'] is True else 'SELL',
                                          timestamp=int(exchange_trade['time']))
                            trades.append(trade)
                        self.repository.process_trades(trades=trades, account=self.account.alias)
                        if len(exchange_trades) < 1:
                            newest_trade_reached = True

                    if newest_trade_reached:  # all trades downloaded
                        # calculate incomes
                        incomes = self.calculate_incomes(symbol=symbol,
                                                         trades=self.repository.get_trades(symbol=symbol,
                                                                                           account=self.account.alias))
                        self.repository.process_incomes(incomes=incomes, account=self.account.alias)
                logger.warning('Synced trades')
            except Exception as e:
                logger.error(f'Failed to process trades: {e}')

            time.sleep(60)

    def calc_long_pprice(self, long_psize, trades: List[TradeEntity]):
        trades.sort(key=lambda x: x.timestamp)
        psize, pprice = 0.0, 0.0
        for trade in trades:
            abs_qty = abs(trade.quantity)
            if trade.side == 'BUY':
                new_psize = psize + abs_qty
                pprice = pprice * (psize / new_psize) + trade.price * (abs_qty / new_psize)
                psize = new_psize
            else:
                psize = max(0.0, psize - abs_qty)
        return pprice

    def calc_long_pnl(self, entry_price, close_price, qty, inverse, c_mult) -> float:
        if inverse:
            if entry_price == 0.0 or close_price == 0.0:
                return 0.0
            return abs(qty) * c_mult * (1.0 / entry_price - 1.0 / close_price)
        else:
            return abs(qty) * (close_price - entry_price)

    def calculate_incomes(self, symbol: str, trades: List[TradeEntity]) -> List[Income]:
        incomes = []
        psize, pprice = 0.0, 0.0
        for trade in trades:
            if trade.side == 'BUY':
                new_psize = psize + trade.quantity
                pprice = pprice * (psize / new_psize) + trade.price * (trade.quantity / new_psize)
                psize = new_psize
            elif psize > 0.0:
                income = Income(symbol=symbol,
                                asset=trade.asset,
                                type='REALIZED_PNL',
                                income=self.calc_long_pnl(pprice, trade.price, trade.quantity, False, 1.0),
                                timestamp=trade.timestamp,
                                transaction_id=trade.order_id)
                incomes.append(income)
                psize = max(0.0, psize - trade.quantity)
        return incomes

    def sync_account(self):
        while True:
            try:
                account = self.rest_manager.get_account()
                current_prices = self.rest_manager.get_all_tickers()
                total_usdt_wallet_balance = 0.0
                total_unrealized_profit = 0.0
                asset_balances = []
                positions = []
                for balance in account['balances']:
                    asset = balance['asset']
                    free = float(balance['free'])
                    locked = float(balance['locked'])
                    asset_quantity = free + locked
                    if asset_quantity > 0.0:
                        if asset in ['USDT', 'BUSD', 'USDC', 'USDP']:
                            total_usdt_wallet_balance += asset_quantity
                        else:
                            current_usd_prices = [p for p in current_prices if
                                                  p['symbol'] in [f'{asset}BTC', f'{asset}USDT', f'{asset}BUSD',
                                                                  f'{asset}USDC', f'{asset}USDP']]
                            if len(current_usd_prices) > 0:
                                asset_usd_balance = 0.0
                                unrealized_profit = 0.0
                                asset_positions = []
                                for current_usd_price in current_usd_prices:
                                    symbol = current_usd_price['symbol']
                                    symbol_trades = self.repository.get_trades_by_asset(symbol,
                                                                                        account=self.account.alias)

                                    if len(symbol_trades) > 0:  # and len(self.repository.get_open_orders(symbol)) > 0:
                                        position_price = self.calc_long_pprice(long_psize=asset_quantity,
                                                                               trades=symbol_trades)

                                        # position size is already bigger than 0, so there is a position
                                        unrealized_profit = (self.get_current_price(
                                            symbol) - position_price) * asset_quantity
                                        total_unrealized_profit += unrealized_profit

                                        position = Position(symbol=symbol,
                                                            entry_price=position_price,
                                                            position_size=asset_quantity,
                                                            side='LONG',
                                                            unrealizedProfit=unrealized_profit,
                                                            initial_margin=0.0)
                                        asset_positions.append(position)
                                        logger.debug(f'Processed position for {symbol}')

                                position_with_open_orders = [position for position in asset_positions
                                                             if len(self.repository.get_open_orders(position.symbol,
                                                                                                    account=self.account.alias)) > 0]
                                selected_position = None
                                if len(position_with_open_orders) == 1:
                                    selected_position = position_with_open_orders[0]
                                elif len(position_with_open_orders) > 1:
                                    selected_position = position_with_open_orders[0]
                                    logger.warning(f'Found multiple different symbols '
                                                   f'({[pos.symbol for pos in position_with_open_orders]}) with open '
                                                   f'orders for asset {asset}, using {selected_position.symbol}')
                                else:
                                    overall_latest_trade_date = None
                                    for position in asset_positions:
                                        symbol_trades = self.repository.get_trades(symbol=position.symbol,
                                                                                   account=self.account.alias)
                                        if len(symbol_trades) > 0:
                                            latest_trade = max([trade.timestamp for trade in symbol_trades])
                                            if overall_latest_trade_date is None or latest_trade > overall_latest_trade_date:
                                                overall_latest_trade_date = latest_trade
                                                selected_position = position
                                                continue

                                if selected_position is not None:
                                    asset_usd_balance = asset_quantity * selected_position.entry_price
                                    positions.append(selected_position)

                                asset_balance = AssetBalance(asset=balance['asset'],
                                                             balance=asset_usd_balance,
                                                             unrealizedProfit=unrealized_profit)
                                asset_balances.append(asset_balance)
                            else:
                                logger.debug(f'NO PRICE FOUND FOR ASSET {asset}')

                positions_to_use = []
                for position in positions:
                    base_asset = self.get_asset(position.symbol)
                    quote_asset = self.get_quote_asset(position.symbol)

                    quote_based_position_found = False

                    for inspected_position in positions:
                        inspected_base_asset = self.get_asset(inspected_position.symbol)
                        inspected_quote_asset = self.get_quote_asset(inspected_position.symbol)
                        if inspected_quote_asset == base_asset and \
                                len(self.repository.get_trades(symbol=inspected_position.symbol,
                                                               account=self.account.alias)) > 0:
                            [positions_to_use.remove(p) for p in positions_to_use
                             if self.get_asset(p.symbol) == inspected_quote_asset
                             and self.get_quote_asset(p.symbol) == inspected_base_asset]

                            quote_based_position_found = True
                            break

                    if not quote_based_position_found:
                        positions_to_use.append(position)

                coin_usdt_balance = sum([b.balance for b in asset_balances])
                total_usdt_wallet_balance += coin_usdt_balance
                logger.info(f"Total wallet balance in USDT = {total_usdt_wallet_balance}")

                total_balance = Balance(totalBalance=total_usdt_wallet_balance,
                                        totalUnrealizedProfit=total_unrealized_profit,
                                        assets=asset_balances)

                self.repository.process_balances(total_balance, account=self.account.alias)
                self.repository.process_positions(positions_to_use, account=self.account.alias)
                logger.warning('Synced account')
            except Exception as e:
                logger.error(f'Failed to process balance: {e}')

            time.sleep(20)

    def sync_open_orders(self):
        while True:
            orders = []
            try:
                open_orders = self.rest_manager.get_open_orders()
                for open_order in open_orders:
                    order = Order()
                    order.symbol = open_order['symbol']
                    order.price = float(open_order['price'])
                    order.quantity = float(open_order['origQty'])
                    order.side = open_order['side']
                    order.position_side = 'LONG'
                    order.type = open_order['type']
                    orders.append(order)
            except Exception as e:
                logger.error(f'Failed to process open orders for symbol: {e}')
            self.repository.process_orders(orders, account=self.account.alias)

            logger.warning('Synced open orders')

            time.sleep(30)

    def get_current_price(self, symbol: str) -> float:
        if symbol not in self.tick_symbols:
            symbol_trade_thread = threading.Thread(
                name=f'trade_thread_{symbol}', target=self.process_trades, args=(symbol,), daemon=True)
            symbol_trade_thread.start()

        curr_price = self.repository.get_current_price(symbol, account=self.account.alias)
        return curr_price.price if curr_price else 0.0

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
            # Price update every 5 seconds is fast enough
            time.sleep(5)
        logger.warning('Stopped trade-stream processing')
        self.tick_symbols.remove(symbol)
