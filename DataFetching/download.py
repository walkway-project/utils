from helpers.downloader import download 
import os
from pathlib import Path
from helpers.parser import process_download 
from metadata.metadata import MetadataGenerator
from metadata.mdtimeframe import TimeFrame
import warnings

class PathWarning(UserWarning): pass

metadataGenerator = MetadataGenerator()

def initializeSymbol(symbol, catalystBase):
    print(f"Starting Processing for {symbol}")
    dpath = download(symbol, catalystBase)
    process_download(dpath, catalystBase)

def initializeSymbols(symbolList):
    env_value = os.environ.get('DATA_WAREHOUSE')
    if not env_value:
        catalystBase = Path.cwd().parent.parent.joinpath("Data")
        warnings.warn("No $DATA_WAREHOUSE found, using default {catalystBase}.", PathWarning)
    else:
        catalystBase = Path(env_value)
    mdSymbolList = []
    if catalystBase.joinpath("metadata.json").is_file() and os.path.getsize(catalystBase.joinpath("metadata.json")) != 0:
        print("Metadata Found. Performing Efficient Downloads.")
        md = metadataGenerator.read_metadata(catalystBase, "data")
        mdSymbolList = md.get("symbols", [])
        mdTimeframe = md.get("timeframe", -1)
    else:
        print("No Metadata Found. Falling back to naive download.")
    for symbol in symbolList:
        if symbol not in mdSymbolList:
            initializeSymbol(symbol, catalystBase)
    print("Writing Metadata")
    metadataGenerator.generate_download_metadata(symbolList, TimeFrame.TEST)
    metadataGenerator.author_metadata(catalystBase)
    print("Downloads Complete!")


if __name__ == "__main__":
    tickers = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "DOGEUSDT", "MATICUSDT", "BNBUSDT", "XRPUSDT", "DOTUSDT", "SOLUSDT"]
    initializeSymbols(tickers)
