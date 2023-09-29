import orjson as json
from metadata.mdtimeframe import TimeFrame
from pathlib import Path

class MetadataGenerator:

    def __init__(self):
        self.metadata = {} #schema: type -> dict containing specific data
        self.features = set() 
        self.symbols = set() 


        self.FEATURE_KEY = "features"
        self.SYMBOL_KEY = "symbols"
        self.TIMEFRAME_KEY = "timeframe"
        self.OHLCV_FNAME = "ohlcv"
        self.ORDERBOOK_FNAME = "orderbook"

        self.downloaded = False

    #we are going to count everything as features.

    def generate_download_metadata(self, symbols, timeframe:TimeFrame):
        """
        ONLY called after downloading. Sets symbols, timeframe, and features. 
        """
        if self.downloaded:
            raise Exception("Can only generate download metadata once!")
        self.symbols = set(symbols)
        self.features = set([self.OHLCV_FNAME, self.ORDERBOOK_FNAME])
        self.metadata[self.SYMBOL_KEY] = symbols
        self.metadata[self.TIMEFRAME_KEY] = timeframe
        self.downloaded = True



    def generate_feature_metadata(self, features:set):
        """
        Stores metadata for all features and their timeframes for symbols.
        """
        self.metadata[features] = self.metadatafeatures.union(features)

    def set_metadata(self, metadata):
        """
        Sets the metadata to a given directory. NOT ADVISED. Assumes metadata is not malformed.
        """
        self.metadata = metadata
        try:
            self.symbols = set(metadata[self.SYMBOL_KEY])
        except:
            raise Exception("Could not parse the provided metadata!")

    def read_metadata(self, path, callback = False):
        """
        Extracts metadata from Path. You can supply spcific types or "all".
        """
        with open(path.joinpath("metadata.json"), "r") as json_file:
            data = json_file.read()
        mdDict = json.loads(data)
        if callback:
            return mdDict
        else:
            self.metadata = mdDict


    def author_metadata(self, path):
        """
        Stores the current metadata into the path/metadata.json.
        """
        with open(path.joinpath("metadata.json"), "a") as jsonfile:
            jsonfile.write(json.dumps(self.metadata).decode("utf-8"))



