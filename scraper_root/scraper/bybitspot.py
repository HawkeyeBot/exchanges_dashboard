import datetime
import logging
import threading
import time
from typing import List

from pybit.unified_trading import HTTP

from scraper_root.scraper.data_classes import AssetBalance, Position, Tick, Balance, Income, Order, Trade, Account
from scraper_root.scraper.persistence.orm_classes import TradeEntity
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


class BybitSpot:
    def __init__(self, account: Account, symbols: List[str], repository: Repository):
        logger.info(f"Bybit Spot initializing")
        self.account = account
        self.alias = self.account.alias
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.repository = repository

        # Bybit unified trading client
        self.client = HTTP(testnet=False, api_key=self.api_key, api_secret=self.secret)

        # Verify API connection
        try:
            test = self.client.get_wallet_balance(accountType="UNIFIED")
            if test['retCode'] == 0:
                logger.info(f"{self.alias}: REST login successful")
            else:
                logger.error(f"{self.alias}: Failed to login - {test.get('retMsg')}")
                raise SystemExit()
        except Exception as e:
            logger.error(f"{self.alias}: Failed to login: {e}")
            raise SystemExit()

        self.tick_symbols = []
        self.all_symbols = []

    def find_new_traded_symbols(self):
        """Automatically discover symbols that have been traded"""
        while True:
            try:
                # Get all spot symbols from Bybit
                if not self.all_symbols:
                    instruments_response = self.client.get_instruments_info(category="spot")
                    if instruments_response['retCode'] == 0:
                        self.all_symbols = [item['symbol'] for item in instruments_response['result']['list']]
                        logger.info(f"{self.alias}: Found {len(self.all_symbols)} spot symbols")

                counter = 0
                for symbol in self.all_symbols:
                    if symbol not in self.repository.get_symbol_checks(account=self.alias):
                        if not self.repository.is_symbol_traded(symbol, account=self.alias) and counter < 10:
                            # Check if this symbol has any trades
                            trades_response = self.client.get_executions(
                                category="spot",
                                symbol=symbol,
                                limit=1
                            )
                            counter += 1
                            self.repository.process_symbol_checked(symbol, account=self.alias)

                            if trades_response['retCode'] == 0:
                                trades = trades_response['result'].get('list', [])
                                if len(trades) > 0:
                                    logger.info(f'{self.alias}: Trades found for {symbol}, adding to sync list')
                                    self.repository.process_traded_symbol(symbol, account=self.alias)

                logger.info(f'{self.alias}: Updated new traded symbols')
                time.sleep(30)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to verify unchecked symbols: {e}')
                time.sleep(60)

    def start(self):
        logger.info(f'{self.alias}: Starting Bybit Spot scraper')

        # Start symbol discovery thread
        symbol_search_thread = threading.Thread(
            name=f'symbol_search_thread', target=self.find_new_traded_symbols, daemon=True)
        symbol_search_thread.start()

        # Start balance sync thread
        sync_balance_thread = threading.Thread(
            name=f'sync_balance_thread', target=self.sync_account, daemon=True)
        sync_balance_thread.start()

        # Start trades sync thread
        sync_trades_thread = threading.Thread(
            name=f'sync_trades_thread', target=self.sync_trades, daemon=True)
        sync_trades_thread.start()

        # Start orders sync thread
        sync_orders_thread = threading.Thread(
            name=f'sync_orders_thread', target=self.sync_open_orders, daemon=True)
        sync_orders_thread.start()

    def sync_account(self):
        while True:
            try:
                # Get unified wallet balance
                wallet_response = self.client.get_wallet_balance(accountType="UNIFIED")

                if wallet_response['retCode'] != 0:
                    logger.error(f"{self.alias}: Failed to get wallet balance: {wallet_response.get('retMsg')}")
                    time.sleep(60)
                    continue

                wallet_data = wallet_response['result']['list'][0]
                total_equity = float(wallet_data.get('totalEquity', 0))
                total_wallet_balance = float(wallet_data.get('totalWalletBalance', 0))
                total_unrealized_pnl = total_equity - total_wallet_balance

                asset_balances = []
                positions = []

                # Process each coin in the wallet
                for coin_data in wallet_data.get('coin', []):
                    coin = coin_data.get('coin')
                    wallet_balance = float(coin_data.get('walletBalance', 0))
                    equity = float(coin_data.get('equity', 0))
                    unrealized_pnl = equity - wallet_balance

                    if wallet_balance > 0 or equity > 0:
                        # Skip stablecoins for position tracking
                        if coin not in ['USDT', 'USDC', 'BUSD', 'USDP']:
                            # Try to find position for this asset
                            symbol = f"{coin}USDT"
                            symbol_trades = self.repository.get_trades(symbol=symbol, account=self.alias)

                            if len(symbol_trades) > 0:
                                position_price = self.calc_long_pprice(long_psize=wallet_balance, trades=symbol_trades)

                                # Get current price
                                current_price = self.get_current_price(symbol)
                                if current_price > 0:
                                    calc_unrealized_pnl = (current_price - position_price) * wallet_balance

                                    position = Position(
                                        symbol=symbol,
                                        entry_price=position_price,
                                        position_size=wallet_balance,
                                        side='LONG',
                                        unrealizedProfit=calc_unrealized_pnl,
                                        initial_margin=0.0,
                                        market_price=current_price
                                    )
                                    positions.append(position)

                        asset_balance = AssetBalance(
                            asset=coin,
                            balance=wallet_balance,
                            unrealizedProfit=unrealized_pnl
                        )
                        asset_balances.append(asset_balance)

                # Create balance object
                balance = Balance(
                    totalBalance=total_wallet_balance,
                    totalUnrealizedProfit=total_unrealized_pnl,
                    assets=asset_balances
                )

                self.repository.process_balances(balance=balance, account=self.alias)
                self.repository.process_positions(positions=positions, account=self.alias)
                logger.warning(f'{self.alias}: Synced account')

                time.sleep(30)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process balance: {e}')
                time.sleep(60)

    def sync_trades(self):
        """Sync trade history for all traded symbols"""
        first_trade_reached = {}
        max_downloads = 10

        while True:
            try:
                # Get next symbol to sync (iterates through all traded symbols)
                symbol = self.repository.get_next_traded_symbol(account=self.alias)

                if symbol is None:
                    logger.info(f"{self.alias}: No traded symbols to sync")
                    time.sleep(120)
                    continue

                # Mark this symbol as being downloaded
                self.repository.update_trades_last_downloaded(symbol=symbol, account=self.alias)
                logger.warning(f'{self.alias}: Updating trades for {symbol}')

                traded_symbols = [symbol]

                for symbol in traded_symbols:
                    if symbol not in first_trade_reached:
                        first_trade_reached[symbol] = False

                    counter = 0

                    # Fetch older trades
                    while not first_trade_reached[symbol] and counter < max_downloads:
                        counter += 1
                        oldest_trade = self.repository.get_oldest_trade(symbol=symbol, account=self.alias)

                        if oldest_trade is None:
                            end_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
                        else:
                            end_time = oldest_trade.timestamp - 1
                            logger.warning(f'{self.alias}: Synced trades before {end_time} for {symbol}')

                        # Fetch trades from Bybit
                        trades_response = self.client.get_executions(
                            category="spot",
                            symbol=symbol,
                            limit=100,
                            endTime=end_time
                        )

                        if trades_response['retCode'] != 0:
                            logger.error(f"{self.alias}: Failed to get trades for {symbol}: {trades_response.get('retMsg')}")
                            break

                        exchange_trades = trades_response['result'].get('list', [])
                        logger.info(f"{self.alias}: Fetched {len(exchange_trades)} older trades for {symbol}")

                        trades = []
                        for trade_data in exchange_trades:
                            trade = Trade(
                                symbol=trade_data['symbol'],
                                asset=self.get_base_asset(trade_data['symbol']),
                                order_id=int(trade_data['orderId']),
                                quantity=float(trade_data['execQty']),
                                price=float(trade_data['execPrice']),
                                type='REALIZED_PNL',
                                side=trade_data['side'].upper(),
                                timestamp=int(trade_data['execTime'])
                            )
                            trades.append(trade)

                        self.repository.process_trades(trades=trades, account=self.alias)

                        if len(exchange_trades) < 100:
                            first_trade_reached[symbol] = True

                    # Fetch newer trades
                    newest_trade_reached = False
                    while not newest_trade_reached and counter < max_downloads:
                        counter += 1
                        newest_trade = self.repository.get_newest_trade(symbol=symbol, account=self.alias)

                        if newest_trade is None:
                            start_time = int(datetime.datetime.fromisoformat('2020-01-01 00:00:00+00:00').timestamp() * 1000)
                        else:
                            start_time = newest_trade.timestamp + 1
                            logger.warning(f'{self.alias}: Synced newer trades since {start_time} for {symbol}')

                        trades_response = self.client.get_executions(
                            category="spot",
                            symbol=symbol,
                            limit=100,
                            startTime=start_time
                        )

                        if trades_response['retCode'] != 0:
                            logger.error(f"{self.alias}: Failed to get trades for {symbol}")
                            break

                        exchange_trades = trades_response['result'].get('list', [])
                        logger.info(f"{self.alias}: Fetched {len(exchange_trades)} newer trades for {symbol}")

                        trades = []
                        for trade_data in exchange_trades:
                            trade = Trade(
                                symbol=trade_data['symbol'],
                                asset=self.get_base_asset(trade_data['symbol']),
                                order_id=int(trade_data['orderId']),
                                quantity=float(trade_data['execQty']),
                                price=float(trade_data['execPrice']),
                                type='REALIZED_PNL',
                                side=trade_data['side'].upper(),
                                timestamp=int(trade_data['execTime'])
                            )
                            trades.append(trade)

                        self.repository.process_trades(trades=trades, account=self.alias)

                        if len(exchange_trades) < 100:
                            newest_trade_reached = True

                    # Calculate and process incomes after all trades synced
                    if newest_trade_reached:
                        incomes = self.calculate_incomes(
                            symbol=symbol,
                            trades=self.repository.get_trades(symbol=symbol, account=self.alias)
                        )
                        self.repository.process_incomes(incomes=incomes, account=self.alias)

                logger.warning(f'{self.alias}: Synced trades')
                time.sleep(120)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process trades: {e}')
                time.sleep(120)

    def sync_open_orders(self):
        """Sync open orders"""
        while True:
            orders = []
            try:
                # Get all open orders for spot
                orders_response = self.client.get_open_orders(category="spot")

                if orders_response['retCode'] != 0:
                    logger.error(f"{self.alias}: Failed to get open orders: {orders_response.get('retMsg')}")
                    time.sleep(60)
                    continue

                open_orders = orders_response['result'].get('list', [])

                for order_data in open_orders:
                    order = Order()
                    order.symbol = order_data['symbol']
                    order.price = float(order_data['price'])
                    order.quantity = float(order_data['qty'])
                    order.side = order_data['side'].upper()
                    order.position_side = 'LONG'  # Spot is always LONG
                    order.type = order_data['orderType']
                    orders.append(order)

                self.repository.process_orders(orders=orders, account=self.alias)
                logger.warning(f'{self.alias}: Synced open orders')

                time.sleep(30)
            except Exception as e:
                logger.error(f'{self.alias}: Failed to process open orders: {e}')
                time.sleep(60)

    def get_base_asset(self, symbol: str) -> str:
        """Extract base asset from symbol (e.g., BTCUSDT -> BTC)"""
        # Common quote assets
        for quote in ['USDT', 'USDC', 'BUSD', 'BTC', 'ETH']:
            if symbol.endswith(quote):
                return symbol[:-len(quote)]
        return symbol

    def calc_long_pprice(self, long_psize, trades: List[TradeEntity]):
        """Calculate average entry price for long position"""
        trades.sort(key=lambda x: x.timestamp)
        psize, pprice = 0.0, 0.0
        for trade in trades:
            abs_qty = abs(trade.quantity)
            if trade.side == 'BUY':
                new_psize = psize + abs_qty
                if new_psize > 0:
                    pprice = pprice * (psize / new_psize) + trade.price * (abs_qty / new_psize)
                psize = new_psize
            else:
                psize = max(0.0, psize - abs_qty)
        return pprice

    def calc_long_pnl(self, entry_price, close_price, qty) -> float:
        """Calculate P&L for long position"""
        return abs(qty) * (close_price - entry_price)

    def calculate_incomes(self, symbol: str, trades: List[TradeEntity]) -> List[Income]:
        """Calculate realized P&L from trades"""
        incomes = []
        psize, pprice = 0.0, 0.0
        asset = self.get_base_asset(symbol)

        for trade in trades:
            if trade.side == 'BUY':
                new_psize = psize + trade.quantity
                if new_psize > 0:
                    pprice = pprice * (psize / new_psize) + trade.price * (trade.quantity / new_psize)
                psize = new_psize
            elif psize > 0.0:
                income = Income(
                    symbol=symbol,
                    asset=asset,
                    type='REALIZED_PNL',
                    income=self.calc_long_pnl(pprice, trade.price, trade.quantity),
                    timestamp=trade.timestamp,
                    transaction_id=trade.order_id
                )
                incomes.append(income)
                psize = max(0.0, psize - trade.quantity)
        return incomes

    def get_current_price(self, symbol: str) -> float:
        """Get current price for symbol"""
        try:
            ticker_response = self.client.get_tickers(category="spot", symbol=symbol)
            if ticker_response['retCode'] == 0 and ticker_response['result']['list']:
                return float(ticker_response['result']['list'][0]['lastPrice'])
        except Exception as e:
            logger.debug(f"{self.alias}: Failed to get price for {symbol}: {e}")
        return 0.0
