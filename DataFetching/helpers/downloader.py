# pip install tardis-dev
# requires Python >=3.6
from tardis_dev import datasets
from pathlib import Path
import nest_asyncio
import shutil
import os

start_dates = {
        -1 : "2023-07-01",
        1 : "2022-01-01",
        2 : "2021-01-01",
        3 : "2020-01-01",
        }
end_dates = {
        -1 : "2023-08-01",
        1 : "2023-07-01",
        2 : "2023-07-01",
        3 : "2023-07-01",
        }

def download(symbol, catalystBase, timeframe):
    nest_asyncio.apply()
    SYMBOL = symbol
    datasets.download(
        exchange="binance",
        data_types=[
            "incremental_book_L2",
            "trades",
            "book_snapshot_25",
        ],
        from_date=start_date,
        to_date=end_date,
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
