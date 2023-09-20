from helpers.downloader import download 
import os
from pathlib import Path
from helpers.parser import process_download 
from metadata.metadata import write_metadata, read_metadata 
from metadata.mdtimeframe import TimeFrame

def initializeSymbol(symbol, catalystBase, timeframe):
    print(f"Starting Processing for {symbol}")
    dpath = download(symbol, catalystBase, timeframe)
    process_download(dpath, catalystBase)

def initializeSymbols(symbolList, timeframe):
    env_value = os.environ.get('DATA_WAREHOUSE')
    if not env_value:
        default_value = "DATA_WAREHOUSE"
        prompt = f"The environment variable is not set.\n Do you want to use the default '{default_value}'? (y/n): "
        confirmation = input(prompt).strip().lower()
        if confirmation == 'yes' or confirmation == 'y':
            catalystBase = Path(os.path.join('home',' Data'))
        else:
            print('Exiting download job.')
            return
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
            initializeSymbol(symbol, catalystBase, timeframe)
    print("Writing Metadata")
    write_metadata("data", catalystBase, symbolList, TimeFrame.SHORT)
    print("Downloads Complete!")


if __name__ == "__main__":
    initializeSymbols(["XRPUSDT"], TimeFrame.TEST)

