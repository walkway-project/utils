from tardis_dev import datasets
from catalyst.constants import TIMEFRAME_MAP
from pathlib import Path
import nest_asyncio
import shutil
import os

from catalyst.utils.utils.DataFetching.metadata.mdtimeframe import TimeFrame

def download(symbol, catalystBase, timeFrame):
    start, end = TIMEFRAME_MAP[timeFrame]
    nest_asyncio.apply()
    SYMBOL = symbol
    datasets.download(
        exchange="binance",
        data_types=[
            "incremental_book_L2",
            "trades",
            "book_snapshot_25",
        ],
        from_date=start,
        to_date=end,
        symbols=[SYMBOL],
        concurrency=os.cpu_count()/2, #beta af man 
        api_key="",
    )
    targetPath = catalystBase.joinpath(SYMBOL[0:(len(SYMBOL)-4)]+"-SPOT")
    shutil.copytree(Path.cwd().joinpath("datasets"), targetPath)
    shutil.rmtree(Path.cwd().joinpath("datasets"))
    print(f"Finished downloading for {symbol}.")
    return targetPath
