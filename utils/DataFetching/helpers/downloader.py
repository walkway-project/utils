from catalyst.constants import _DOWNLOAD_TIMEFRAME_MAP, MARKET_MAP
from catalyst.utils.utils.DataFetching.metadata.mdtimeframe import TimeFrame
from catalyst.utils.utils.DataFetching.markets import Market
from catalyst.secrets import DATAKEYS

from pathlib import Path
import shutil

import nest_asyncio
from tardis_dev import datasets


def download(symbol:str, catalystBase:Path, timeFrame:TimeFrame, market:Market):
    start, end = _DOWNLOAD_TIMEFRAME_MAP[timeFrame]
    nest_asyncio.apply()
    SYMBOL = symbol
    datasets.download(
        exchange=MARKET_MAP[market],
        data_types=[
            "incremental_book_L2",
            "trades",
            "book_snapshot_25",
        ],
        from_date=start,
        to_date=end,
        symbols=[SYMBOL],
        concurrency=5,
        api_key=DATAKEYS[market],
    )
    targetPath = catalystBase.joinpath(MARKET_MAP[market]).joinpath(SYMBOL)
    shutil.copytree(Path.cwd().joinpath("datasets"), targetPath)
    shutil.rmtree(Path.cwd().joinpath("datasets"))
    print(f"Finished downloading for {symbol}.")
    return targetPath
