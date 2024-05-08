from catalyst.utils.utils.DataFetching.metadata.mdtimeframe import TimeFrame
from catalyst.utils.utils.DataFetching.markets import Market
from catalyst.constants import MARKET_MAP, METADATA_VERSION

import orjson as json

import copy
import os


class MetadataGenerator:
    def __init__(self):
        self.metadata = {} #schema: {market : {data_dict}}
        self.features = {} # schema: {market: list(features)}
        self.symbols = {} #schema: {market : list(symbols)} 
        self.path = ""
        self.lookback = -1

        self.FEATURE_KEY = "features"
        self.SYMBOL_KEY = "symbols"
        self.TIMEFRAME_KEY = "timeframe"
        self.ORDERBOOK_FNAME = "ob_snapshot_50"
        self.TRADES_FNAME = "trades"
        self.UPDATES_FNAME = "l2_updates"
        self.LOOKBACK_KEY = "lookback"
        self.VERSION_KEY = "version"
        self.default_features = [self.TRADES_FNAME, self.ORDERBOOK_FNAME, self.UPDATES_FNAME]

    #we are going to count everything as features.
    def on_download(self, symbol:str, market:Market, timeframe:TimeFrame):
        """
        To be called when downloading a new symbol. 
        """
        market_str = MARKET_MAP[market]
        if market_str not in self.metadata:
            self.metadata[market_str] = {}
            self.metadata[market_str][self.SYMBOL_KEY] = []
        self.metadata[market_str][self.SYMBOL_KEY].append(symbol)
        self.metadata[self.TIMEFRAME_KEY] = timeframe.value
        self.metadata[market_str][self.FEATURE_KEY] = self.default_features

    def on_dispatch(self, features:set, market:Market):
        """
        Stores metadata for all features and their timeframes for symbols. It is recommended to read the metadata beforehand, 
        and to overwrite the metadata afterwards.
        """
        market_str = MARKET_MAP[market]
        self.metadata[market_str][self.FEATURE_KEY] = list(set(self.metadata[market_str][self.FEATURE_KEY]).union(features))
        self.features = self.metadata[market_str][self.FEATURE_KEY]

    def find_uncached_symbols(self, symbols:list[str], market:Market):
        """
        Helper function for determining which symbols have not yet been computed. Must read metadata beforehand.
        """

        result = []
        market_str = MARKET_MAP[market]
        if market_str not in self.metadata:
            self.metadata[market_str] = {}
            self.metadata[market_str][self.SYMBOL_KEY] = []
            return symbols
            
        for symbol in symbols:
            if symbol not in self.metadata[market_str][self.SYMBOL_KEY]:
                result.append(symbol)
        return result
        
    def set_lookback(self, lookback:int):
        self.lookback = lookback
        self.metadata[self.LOOKBACK_KEY] = lookback

    def set_metadata(self, metadata):
        """
        NOT FOR UNADVISED USE - THIS FUNCTION CAN EASILY DESTROY A WAREHOUSE
        Sets the metadata to a given directory. Malformed metadata will result in an exception.
        """
        self.metadata = metadata
        try:
            self.symbols = list(metadata[self.SYMBOL_KEY])
        except:
            raise Exception("Could not parse the provided metadata!")

    def read_metadata(self, path):
        """
        Extracts metadata from Path/metadata.json.
        """
        self.path = path
        with open(path.joinpath("metadata.json"), "r") as json_file:
            data = json_file.read()
        mdDict = json.loads(data)
        if (mdDict.get(self.VERSION_KEY, -1)) != METADATA_VERSION:
            raise Exception("Metadata is out of date - Please regenerate your Data Warehouse!")
        self.metadata = mdDict
        return mdDict
      
    def store_metadata(self, path):
        """
        Stores the current metadata into the path/metadata.json.
        """
        self.path = path.joinpath("metadata.json")

        static_metadata = copy.deepcopy(self.metadata)
        static_metadata[self.VERSION_KEY] = METADATA_VERSION
        for key in static_metadata:
            if isinstance(static_metadata[key], dict):
                static_metadata[key][self.SYMBOL_KEY] = list(static_metadata[key][self.SYMBOL_KEY])

        with open(self.path, "w") as jsonfile:
            jsonfile.truncate(0) #clear file if not clear
            jsonfile.write(json.dumps(static_metadata, option=json.OPT_INDENT_2).decode("utf-8"))

    def delete_metadata(self, path=""):
        """
        Deletes the metadata in the current path, defaults to the cached path. Not intended to be used by end users.
        """
        if not self.path and not path:
            raise Exception("No path stored or found for deletion")
        elif self.path: 
            os.remove(self.path.joinpath("metadata.json"))
        else:  # does not point at md
            path = path.joinpath("metadata.json")
            os.remove(path)

    def overwrite_metadata(self, path):
        """
        Deletes and regenerates the metadata in the given path + "metadata.json".
        """

        self.delete_metadata(path)
        self.store_metadata(path)
        print("Metadata has been updated.")
