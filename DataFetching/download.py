from helpers.downloader import download 
import os
from pathlib import Path
from helpers.parser import process_download 
from metadata.metadata import write_metadata, read_metadata 
from metadata.mdtimeframe import TimeFrame
import warnings

class PathWarning(UserWarning): pass
    
def initializeSymbol(symbol, catalystBase):
    print(f"Starting Processing for {symbol}")
    dpath = download(symbol, catalystBase)
    process_download(dpath, catalystBase)

def initializeSymbols(symbolList):
    env_value = os.environ.get('DATA_WAREHOUSE')
    if not env_value:
        default_value = "DATA_WAREHOUSE"
        warnings.warn("No data warehouse environ detected, using default.", PathWarning)
        catalystBase = Path(os.path.join('/home', 'Data'))
    else:
        catalystBase = Path(env_value)
    mdSymbolList = []
    if catalystBase.joinpath("metadata.json").is_file() and os.path.getsize(catalystBase.joinpath("metadata.json")) != 0:
        print("Metadata Found. Performing Efficient Downloads.")
        md = read_metadata("data", catalystBase)
        mdSymbolList = md.get("symbols", [])
        mdTimeframe = md.get("timeframe", -99)
    else:
        print("No Metadata Found. Falling back to naive download.")
    for symbol in symbolList:
        if symbol not in mdSymbolList:
            initializeSymbol(symbol, catalystBase)
    print("Writing Metadata")
    write_metadata("data", catalystBase, symbolList, TimeFrame.SHORT)
    print("Downloads Complete!")


if __name__ == "__main__":
    initializeSymbols(["XRPUSDT"])

