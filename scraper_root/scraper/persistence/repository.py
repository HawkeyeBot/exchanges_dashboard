import logging
import os
import threading
import time
from datetime import datetime, date, timedelta, timezone
from typing import List, Dict

from sqlalchemy import create_engine, func, Table, asc, nulls_first
from sqlalchemy.orm import sessionmaker

from scraper_root.scraper.data_classes import Order, Tick, Position, Balance, Income, Trade
from scraper_root.scraper.persistence.orm_classes import _DECL_BASE, CurrentPriceEntity, \
    BalanceEntity, AssetBalanceEntity, PositionEntity, IncomeEntity, OrderEntity, DailyBalanceEntity, TradeEntity, \
    TradedSymbolEntity, SymbolCheckEntity

logger = logging.getLogger(__name__)


class Repository:
    def __init__(self):
        self.engine = create_engine(url=os.getenv(
            'DATABASE_PATH', 'sqlite:///data/exchanges_db.sqlite'), echo=False)
        _DECL_BASE.metadata.create_all(self.engine)

        self.session = sessionmaker()
        self.session.configure(bind=self.engine)

        update_daily_balance_thread = threading.Thread(
            name=f'sync_balance_thread', target=self.update_daily_balance, daemon=True)
        update_daily_balance_thread.start()

    def update_daily_balance(self):
        while True:
            try:
                with self.session() as session:
                    session.query(DailyBalanceEntity).delete()
                    session.commit()
                    result = session.query(
                        BalanceEntity.totalWalletBalance).first()
                    current_balance = 0
                    if result is not None:
                        current_balance = result[0]

                daily_balances = []
                with self.engine.connect() as con:
                    oldest_income = self.get_oldest_income()
                    if oldest_income is not None:
                        day = oldest_income.time.date()
                        end_date = date.today()
                        while day <= end_date:
                            rs = con.execute(
                                f'SELECT sum("INCOME"."income") AS "sum" FROM "INCOME" WHERE "INCOME"."time" >= date(\'{day.strftime("%Y-%m-%d")}\')')
                            for row in rs:
                                if row[0] is None:
                                    continue
                                income = float(row[0])
                                daily_balance = DailyBalanceEntity()
                                daily_balance.day = day
                                daily_balance.totalWalletBalance = current_balance - income
                                daily_balances.append(daily_balance)

                            day += timedelta(days=1)

                with self.session() as session:
                    [session.add(balance) for balance in daily_balances]
                    session.commit()
            except Exception as e:
                logger.error(f'Failed to update daily balance: {e}')

            time.sleep(60)

    def process_order_update(self, order: Order):
        # if order is already in the database, update it
        # if order is not in the database, insert it
        pass

    def process_tick(self, tick: Tick):
        with self.session() as session:
            # find entry in database
            query = session.query(CurrentPriceEntity)
            query = query.filter(CurrentPriceEntity.symbol == tick.symbol)
            currentEntity = query.first()
            if currentEntity is None:
                currentEntity = CurrentPriceEntity()
                session.add(currentEntity)

            currentEntity.symbol = tick.symbol
            currentEntity.price = tick.price
            currentEntity.registration_datetime = func.now()
            session.commit()
            logger.debug(f'Tick processed: {tick}')

    def get_current_price(self, symbol: str) -> CurrentPriceEntity:
        with self.session() as session:
            return session.query(CurrentPriceEntity).filter(CurrentPriceEntity.symbol == symbol).first()

    def process_balances(self, balance: Balance):
        with self.session() as session:
            logger.debug('Updating balances')
            session.query(AssetBalanceEntity).delete()
            session.query(BalanceEntity).delete()
            session.commit()

            balanceEntity = BalanceEntity()
            balanceEntity.totalWalletBalance = balance.totalBalance
            balanceEntity.totalUnrealizedProfit = balance.totalUnrealizedProfit

            asset_balance_entities = []
            for asset in balance.assets:
                asset_balance_entity = AssetBalanceEntity()
                asset_balance_entity.asset = asset.asset
                asset_balance_entity.walletBalance = asset.balance
                asset_balance_entity.unrealizedProfit = asset.unrealizedProfit
                asset_balance_entities.append(asset_balance_entity)
            balanceEntity.assets = asset_balance_entities
            session.add(balanceEntity)
            session.commit()

    def process_positions(self, positions: List[Position]):
        with self.session() as session:
            logger.debug('Updating positions')
            session.query(PositionEntity).delete()

            for position in positions:
                position_entity = PositionEntity()
                position_entity.symbol = position.symbol
                position_entity.side = position.side
                position_entity.quantity = position.position_size
                position_entity.entryPrice = position.entry_price
                position_entity.unrealizedProfit = position.unrealizedProfit
                position_entity.initialMargin = position.initial_margin
                session.add(position_entity)
            session.commit()

    def get_oldest_trade(self, symbol: str) -> TradeEntity:
        with self.session() as session:
            logger.debug('Getting oldest trade')
            result = session.query(TradeEntity).filter(TradeEntity.symbol == symbol).order_by(
                TradeEntity.time.asc()).first()
            return result

    def get_trades(self, symbol: str) -> TradeEntity:
        with self.session() as session:
            logger.debug(f'Getting all trades for {symbol}')
            result = session.query(TradeEntity).filter(TradeEntity.symbol == symbol).all()
            return result

    def get_trades_by_asset(self, asset: str) -> TradeEntity:
        with self.session() as session:
            logger.debug(f'Getting all trades for asset {asset}')
            result = session.query(TradeEntity).filter(TradeEntity.symbol.like(f'{asset}%')).all()
            return result

    def get_newest_trade(self, symbol: str) -> TradeEntity:
        with self.session() as session:
            logger.debug('Getting oldest trade')
            result = session.query(TradeEntity).filter(TradeEntity.symbol == symbol).order_by(
                TradeEntity.time.desc()).first()
            return result

    def get_oldest_income(self) -> IncomeEntity:
        with self.session() as session:
            logger.debug('Getting oldest income')
            result = session.query(IncomeEntity).order_by(
                IncomeEntity.time.asc()).first()
            return result

    def get_newest_income(self) -> IncomeEntity:
        with self.session() as session:
            logger.debug('Getting newest income')
            result = session.query(IncomeEntity).order_by(
                IncomeEntity.time.desc()).first()
            return result

    def process_incomes(self, incomes: List[Income]):
        if len(incomes) == 0:
            return
        with self.session() as session:
            logger.warning('Processing incomes')

            session.execute(
                IncomeEntity.__table__.insert(),
                params=[{
                    "transaction_id": income.transaction_id,
                    "symbol": income.symbol,
                    "incomeType": income.type,
                    "income": income.income,
                    "asset": income.asset,
                    "time": datetime.utcfromtimestamp(income.timestamp / 1000),
                    "timestamp": income.timestamp}
                    for income in incomes],
            )
            session.commit()

    def process_trades(self, trades: List[Trade]):
        if len(trades) == 0:
            return
        with self.session() as session:
            logger.warning('Processing trades')

            session.execute(
                TradeEntity.__table__.insert(),
                params=[{
                    "order_id": trade.order_id,
                    "symbol": trade.symbol,
                    "incomeType": trade.type,
                    "asset": trade.asset,
                    "quantity": trade.quantity,
                    "price": trade.price,
                    "side": trade.side,
                    "time": datetime.utcfromtimestamp(trade.timestamp / 1000),
                    "timestamp": trade.timestamp}
                    for trade in trades],
            )
            session.commit()

    def process_orders(self, orders: List[Order]):
        with self.session() as session:
            logger.debug('Processing orders')
            session.query(OrderEntity).delete()

            for order in orders:
                order_entity = OrderEntity()
                order_entity.symbol = order.symbol
                order_entity.price = order.price
                order_entity.type = order.type
                order_entity.quantity = order.quantity
                order_entity.position_side = order.position_side
                order_entity.side = order.side
                order_entity.status = order.status
                session.add(order_entity)
            session.commit()

    def get_open_orders(self, symbol: str) -> List[OrderEntity]:
        with self.session() as session:
            logger.debug(f'Getting orders for {symbol}')
            return session.query(OrderEntity).filter(OrderEntity.symbol == symbol).all()

    def is_symbol_traded(self, symbol: str) -> bool:
        with self.session() as session:
            logger.debug('Getting traded symbol')
            query = session.query(TradedSymbolEntity).filter(TradedSymbolEntity.symbol == symbol)
            return session.query(query.exists()).scalar()

    def get_all_traded_symbols(self) -> List[str]:
        with self.session() as session:
            logger.debug('Getting all traded symbol')
            return [e.symbol for e in session.query(TradedSymbolEntity).all()]

    def get_traded_symbol(self, symbol: str) -> TradedSymbolEntity:
        with self.session() as session:
            logger.debug(f'Getting traded symbol for {symbol}')
            return session.query(TradedSymbolEntity).filter(TradedSymbolEntity.symbol == symbol).first()

    def process_traded_symbol(self, symbol: str):
        with self.session() as session:
            logger.debug('Processing traded symbol')
            traded_symbol_entity = TradedSymbolEntity()
            traded_symbol_entity.symbol = symbol
            session.add(traded_symbol_entity)
            session.commit()

    def update_trades_last_downloaded(self, symbol: str):
        with self.session() as session:
            logger.debug('Updating trades last downloaded')
            traded_symbol = session.query(TradedSymbolEntity).filter(TradedSymbolEntity.symbol == symbol).first()
            traded_symbol.last_trades_downloaded = datetime.now()
            session.commit()

    def get_symbol_checks(self) -> List[SymbolCheckEntity]:
        with self.session() as session:
            logger.debug('Getting all symbol checks')
            return [e.symbol for e in session.query(SymbolCheckEntity).all()]

    def process_symbol_checked(self, symbol: str):
        with self.session() as session:
            existing_symbol_check = session.query(SymbolCheckEntity).filter(SymbolCheckEntity.symbol == symbol).first()
            if existing_symbol_check is None:
                existing_symbol_check = SymbolCheckEntity()
                existing_symbol_check.symbol = symbol
                session.add(existing_symbol_check)
            existing_symbol_check.last_checked_datetime = datetime.now()
            session.commit()

    def get_next_traded_symbol(self) -> TradedSymbolEntity:
        with self.session() as session:
            # first check any of the open positions
            open_positions = self.open_positions()
            for open_position in open_positions:
                position_symbol = open_position.symbol
                traded_symbol = self.get_traded_symbol(position_symbol)
                if traded_symbol.last_trades_downloaded < datetime.utcnow() + timedelta(minutes=5):
                    # open positions are synced at least every 5 minutes
                    return session.query(TradedSymbolEntity).filter(TradedSymbolEntity.symbol == position_symbol).first().symbol
            next_symbol = session.query(TradedSymbolEntity).order_by(nulls_first(TradedSymbolEntity.last_trades_downloaded.asc())).first()
            return next_symbol.symbol

    def open_positions(self) -> List[PositionEntity]:
        with self.session() as session:
            return session.query(PositionEntity).all()
