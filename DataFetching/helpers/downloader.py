# pip install tardis-dev
# requires Python >=3.6
from tardis_dev import datasets
from pathlib import Path
import nest_asyncio
import shutil
import os

def download(symbol, catalystBase):
    nest_asyncio.apply()
    SYMBOL = symbol
    datasets.download(
        exchange="binance",
        data_types=[
            "incremental_book_L2",
            "trades",
            "book_snapshot_25",
        ],
        from_date="2023-06-01",
        to_date="2023-07-01",
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
