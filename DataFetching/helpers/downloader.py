# pip install tardis-dev
# requires Python >=3.6
from tardis_dev import datasets
from pathlib import Path
import nest_asyncio
import shutil
import os
from metadata.mdtimeframe import TimeFrame

TIMEFRAME_MAP = {
    TimeFrame.TEST : ["2023-05-31","2023-07-01"],
    TimeFrame.SHORT : ["2021-12-31", "2023-07-01"],
    TimeFrame.MED : ["2020-12-31", "2023-07-01"],
    TimeFrame.LONG : ["2019-12-31", "2023-07-01"]
}

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
    try:
        os.rename(Path.cwd().joinpath("datasets"), targetPath)
        shutil.rmtree(Path.cwd().joinpath("datasets"))
    except:
        pass
    print(f"Finished downloading for {symbol}.")
    return targetPath


if __name__ == "__main__":
    download("XRPUSDT")
