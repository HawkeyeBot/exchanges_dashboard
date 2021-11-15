import logging
import os
import threading
import time
from datetime import datetime, date, timedelta
from typing import List, Dict

from sqlalchemy import create_engine, func, Table
from sqlalchemy.orm import sessionmaker

from scraper_root.scraper.data_classes import Order, Tick, Position, Balance, Income
from scraper_root.scraper.persistence.orm_classes import _DECL_BASE, CurrentPriceEntity, \
    BalanceEntity, AssetBalanceEntity, PositionEntity, IncomeEntity, OrderEntity, DailyBalanceEntity

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

    def process_orders(self, orders: Dict[str, List[Order]]):
        with self.session() as session:
            logger.debug('Processing orders')
            session.query(OrderEntity).delete()

            for symbol in orders:
                for order in orders[symbol]:
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
