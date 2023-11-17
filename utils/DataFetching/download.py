from catalyst.utils.utils.DataFetching.helpers.downloader import download 
import os
from pathlib import Path
from catalyst.utils.utils.DataFetching.helpers.parser import process_download 
from catalyst.utils.utils.DataFetching.metadata.metadata import MetadataGenerator
from catalyst.utils.utils.DataFetching.metadata.mdtimeframe import TimeFrame

class PathWarning(UserWarning): pass

metadataGenerator = MetadataGenerator()

def initializeSymbol(symbol, catalystBase, timeFrame):
    print(f"Starting Processing for {symbol}")
    dpath = download(symbol, catalystBase, timeFrame)
    process_download(dpath, catalystBase)

def fullDownload(symbolList, catalystBase, timeFrame):
    """
    Downloads all symbols in symbolList to catalystBase for timeFrame.
    """
    for symbol in symbolList:
        initializeSymbol(symbol, catalystBase, timeFrame)
    metadataGenerator.generate_download_metadata(symbolList, timeFrame)
    metadataGenerator.author_metadata(catalystBase)

def initializeSymbols(symbolList, timeFrame):
    """
    Scans symbollist for timeFrame and downloads extra tickers if needed.
    """

    delta = False

    env_value = os.environ.get('DATA_WAREHOUSE')
    if not env_value:
        catalystBase = Path.cwd().parent.parent.parent.joinpath("Data")
        print(f"No $DATA_WAREHOUSE found, using default {catalystBase}.")
    else:
        catalystBase = Path(env_value)
    if not os.path.exists(catalystBase) : os.makedirs(catalystBase)
    if catalystBase.joinpath("metadata.json").is_file() and os.path.getsize(catalystBase.joinpath("metadata.json")) != 0: #md exists
        print("Metadata Found.")
        metadataGenerator.read_metadata(catalystBase)
        if metadataGenerator.metadata[metadataGenerator.TIMEFRAME_KEY] >= timeFrame.value:
            print("Timeframe is usable. Performing Optimized Downloads.")
            for symbol in symbolList:
                if symbol not in metadataGenerator.symbols:
                    initializeSymbol(symbol, catalystBase, timeFrame)
                    metadataGenerator.metadata.add(symbol)
                    delta = True
        else:
            print("Extending Timeframe. Performing Naive Download.")
            fullDownload(symbolList=symbolList, catalystBase=catalystBase, timeFrame=timeFrame)
    else: #metadata does not exist
        print("No Metadata Found. Falling back to naive download.")
        fullDownload(symbolList=symbolList, catalystBase=catalystBase, timeFrame=timeFrame)

    if delta: #if we have md but need to add a symbol: remove old md, write new.
        print("Writing Metadata")
        metadataGenerator.delete_metadata()
        metadataGenerator.author_metadata(catalystBase)
    print("Downloads Complete!")


if __name__ == "__main__":
    tickers = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    initializeSymbols(tickers, TimeFrame.MED)
