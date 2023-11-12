import orjson as json
import os
from catalyst.utils.utils.DataFetching.metadata.mdtimeframe import TimeFrame

class MetadataGenerator:

    def __init__(self):
        self.metadata = {} #schema: type -> dict containing specific data
        self.features = set() 
        self.symbols = set() 
        self.path = ""

        self.FEATURE_KEY = "features"
        self.SYMBOL_KEY = "symbols"
        self.TIMEFRAME_KEY = "timeframe"
        self.ORDERBOOK_FNAME = "ob_snapshot_50"
        self.TRADES_FNAME = "trades"
        self.UPDATES_FNAME = "l2_updates"

        self.downloaded = False

    #we are going to count everything as features.

    def generate_download_metadata(self, symbols, timeframe:TimeFrame):
        """
        ONLY called after downloading. Sets symbols, timeframe, and features. 
        """
        if self.downloaded:
            raise Exception("Can only generate download metadata once!")
        self.symbols = set(symbols)
        self.features = set([self.TRADES_FNAME, self.ORDERBOOK_FNAME, self.UPDATES_FNAME])
        self.metadata[self.SYMBOL_KEY] = symbols
        self.metadata[self.TIMEFRAME_KEY] = timeframe
        self.downloaded = True

    def generate_feature_metadata(self, features:set):
        """
        Stores metadata for all features and their timeframes for symbols.
        """
        self.metadata[self.FEATURE_KEY] = list(set(self.metadata[self.FEATURE_KEY]).union(features))
        self.features = self.metadata[self.FEATURE_KEY]

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
        Extracts metadata from Path/metadata.json.
        """
        self.path = path
        with open(path.joinpath("metadata.json"), "r") as json_file:
            data = json_file.read()
        mdDict = json.loads(data)
        self.metadata = mdDict
        self.features = mdDict[self.FEATURE_KEY]
        self.symbols = set(self.metadata[self.SYMBOL_KEY])
        if callback:
            return mdDict


    def author_metadata(self, path):
        """
        Stores the current metadata into the path/metadata.json.
        """
        self.path = path.joinpath("metadata.json")
        self.metadata[self.FEATURE_KEY] = list(self.features)
        with open(self.path, "a") as jsonfile:
            jsonfile.write(json.dumps(self.metadata).decode("utf-8"))

    def delete_metadata(self, path = ""):
        """
        Deletes the metadata in the current path, defaults to the cached path. Not intended to be used by end users.
        """
        if not self.path and not path:
            raise Exception("No path stored or found for deletion")
        elif self.path: 
            print(f"Deleting metadata stored in {self.path}")
            os.remove(self.path.joinpath("metadata.json"))
        else: #does not point at md
            path = path.joinpath("metadata.json")
            os.remove(path)

    def overwrite_metadata(self, path):
        """
        Deletes and regenerates the metadata in the given path + "metadata.json".
        """
        self.delete_metadata(path.joinpath("metadata.json"))
        self.author_metadata(path)



