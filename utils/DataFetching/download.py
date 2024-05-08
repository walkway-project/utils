from catalyst.utils.utils.DataFetching.helpers.downloader import download 
from catalyst.utils.utils.DataFetching.helpers.parser import process_download 
from catalyst.utils.utils.DataFetching.metadata.metadata import MetadataGenerator
from catalyst.utils.utils.DataFetching.metadata.mdtimeframe import TimeFrame
from catalyst.utils.utils.DataFetching.markets import Market
from catalyst.grail.grail.domains.domain_enum import DomainType
from catalyst.constants import DEFAULTPATH, MARKET_MAP, DOMAIN_MAP

import os
from pathlib import Path

class PathWarning(UserWarning): pass

metadataGenerator = MetadataGenerator()

def download_symbols(symbols:list[str], timeframe:TimeFrame, market:Market, domain:DomainType=DomainType.TIME_1S):
    env_value = os.environ.get("DATA_WAREHOUSE")
    if not env_value:
        catalystBase = DEFAULTPATH
        print(f"No $DATA_WAREHOUSE found, using default {catalystBase}.")
    else:
        catalystBase = Path(env_value)

    if not os.path.exists(catalystBase):
        os.makedirs(catalystBase)

    if catalystBase.joinpath("metadata.json").is_file() and os.path.getsize(catalystBase.joinpath("metadata.json")) != 0: #md exists
        print("Metadata Found.")
        metadataGenerator.read_metadata(catalystBase)
        if metadataGenerator.metadata[metadataGenerator.TIMEFRAME_KEY] == timeframe.value:
            print("Timeframe is usable. Performing Optimized Downloads.")
            for symbol in metadataGenerator.find_uncached_symbols(symbols, market):
                downloaded_path = download(symbol, catalystBase, timeframe, market)
                process_download(symbol, downloaded_path)
                metadataGenerator.on_download(symbol=symbol, market=market, timeframe=timeframe)
        metadataGenerator.overwrite_metadata(catalystBase)
    else: #fresh download
        print("Performing fresh download of requested symbols.")
        for symbol in symbols:
            downloaded_path = download(symbol, catalystBase, timeframe, market)
            process_download(symbol, downloaded_path)
            metadataGenerator.on_download(symbol=symbol, market=market, timeframe=timeframe)
    metadataGenerator.store_metadata(catalystBase)

if __name__ == "__main__":
    tickers = ["SOLUSDT", "XRPUSDT", "DOGEUSDT", "BTCUSDT", "ETHUSDT", "ADAUSDT"]
    tickers = ["XRPUSDT", "DOGEUSDT"]
    tickers = ["SOLUSDT"]
    market = Market.BINANCE_USDT_FUTURES
    download_symbols(tickers, TimeFrame.TEST, market)
