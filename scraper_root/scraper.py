import os
from types import SimpleNamespace

import hjson

from scraper.binancefutures import BinanceFutures
from scraper_root.scraper.data_classes import ScraperConfig
from scraper_root.scraper.persistence.repository import Repository

if __name__ == '__main__':
    config_file_path = os.environ['CONFIG_FILE']
    with open(config_file_path) as config_file:
        user_config = hjson.load(config_file, object_hook=lambda d: SimpleNamespace(**d))

    scraper_config = ScraperConfig()
    for key in user_config:
        if hasattr(scraper_config, key):
            setattr(scraper_config, key, user_config[key])

    if 'BTCUSDT' not in scraper_config.symbols:
        scraper_config.symbols.append('BTCUSDT')

    scraper = None
    repository=Repository()
    if scraper_config.exchange == 'binance_futures':
        scraper = BinanceFutures(config=scraper_config, repository=repository)
    else:
        raise Exception(f'Exchange {user_config.exchange} not implemented')

    scraper.start()
