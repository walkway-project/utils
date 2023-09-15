from helpers.downloader import download 
import os
from pathlib import Path
from helpers.parser import process_download 
from metadata.metadata import write_metadata, read_metadata 

def initializeSymbol(symbol):
    print(f"Starting Processing for {symbol}")
    dpath = download(symbol)
    process_download(dpath)

def initializeSymbols(symbolList):
    catalystBase = Path.cwd().parent.parent.joinpath("Data")
    mdSymbolList = []
    if catalystBase.joinpath("metadata.json").is_file() and os.path.getsize(catalystBase.joinpath("metadata.json")) != 0:
        print("Metadata Found. Performing Efficient Downloads.")
        mdSymbolList = read_metadata("data", catalystBase)
    else:
        print("No Metadata Found. Falling back to naive download.")
    for symbol in symbolList:
        if symbol not in mdSymbolList:
            initializeSymbol(symbol)
    print("Writing Metadata")
    write_metadata("data", catalystBase, symbolList)
    print("Downloads Complete!")


if __name__ == "__main__":
    initializeSymbols(["XRPUSDT"])

