import logging
import os
import time
from types import SimpleNamespace
from typing import List

import hjson
from scraper.binancefutures import BinanceFutures
from scraper.bybitderivatives import BybitDerivatives
from scraper_root.scraper.binancespot import BinanceSpot
from scraper_root.scraper.data_classes import ScraperConfig, Account
from scraper_root.scraper.persistence.repository import Repository

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger()

if __name__ == '__main__':
    config_file_path = os.environ.get('CONFIG_FILE', 'config.json')
    logger.info(f"Using config file {config_file_path}")
    with open(config_file_path) as config_file:
        user_config = hjson.load(config_file, object_hook=lambda d: SimpleNamespace(**d))

    scraper_config = ScraperConfig()
    for key in user_config:
        if hasattr(scraper_config, key):
            setattr(scraper_config, key, user_config[key])
    parsed_accounts = []
    for account in scraper_config.accounts:
        parsed_accounts.append(Account(**account))
    scraper_config.accounts = parsed_accounts

    if 'BTCUSDT' not in scraper_config.symbols:
        scraper_config.symbols.append('BTCUSDT')

    scrapers: List = []
    repository = Repository(accounts=[account.alias for account in scraper_config.accounts])
    scraper = None
    for account in scraper_config.accounts:
        if account.exchange == 'binance_futures':
            scraper = BinanceFutures(account=account, symbols=scraper_config.symbols, repository=repository)
        elif account.exchange == 'binance_spot':
            scraper = BinanceSpot(account=account, symbols=scraper_config.symbols, repository=repository)
        elif account.exchange == 'bybit_derivatives':
            scraper = BybitDerivatives(account=account, symbols=scraper_config.symbols, repository=repository)
        else:
            raise Exception(f'Encountered unsupported exchange {account.exchange}')

        try:
            scraper.start()
            logger.info(f'Started scraping account {account.alias} ({account.exchange})')
        except Exception as e:
            logger.error(f"Failed to start exchange: {e}")

    while True:
        try:
            time.sleep(10)
        except:
            break

    logger.info('Scraper shut down')
