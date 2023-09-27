import orjson as json
from metadata.mdtimeframe import TimeFrame
from pathlib import Path

class MetadataGenerator:

    def __init__(self):
        self.metadata = {} #schema: type -> dict containing specific data
        self.symbols = set() 
        self.DATAKEY = "data"
        self.FEATUREKEY = "features"
        self.metadata[self.DATAKEY] = {}
        self.metadata[self.FEATUREKEY] = {}

    def generate_download_metadata(self, symbols, timeframe:TimeFrame):
        """
        Stores metadata for symbols + timeframe.
        """
        data = self.metadata[self.DATAKEY]
        data["symbols"] = symbols
        self.symbols = set(symbols)
        data["timeframe"] = timeframe

    def generate_feature_metadata(self, features, timeframe:TimeFrame, symbols = []):
        """
        Stores metadata for all features and their timeframes for given symbols. If the download metadata
        has already been generated, this will use the same symbols.
        """
        if not (self.symbols or symbols):
            raise Exception("Symbols must be defined")
        data = self.metadata[self.FEATUREKEY]
        data[features] = features
        if not self.symbols:
            self.metadata[self.DATAKEY]["symbols"] = symbols
            self.symbols = set(symbols)
        data[timeframe] = timeframe

    def set_metadata(self, metadata):
        """
        Sets the metadata to a given directory. NOT ADVISED. Assumes metadata is not malformed.
        """
        self.metadata = metadata
        try:
            self.symbols = set(metadata[self.DATAKEY]["symbols"])
        except:
            raise Exception("Metadata is malformed!")

    def read_metadata(self, path, type):
        """
        Extracts metadata from Path. You can supply spcific types or "all".
        """
        if (type not in self.metadata.keys() and type != "all"):
            raise Exception("Metadata type is not supported!")
        with open(directory.joinpath("metadata.json"), "r") as json_file:
            data = json_file.read()
        mdDict = json.loads(data)
        if (type == "all"):
            return mdDict
        else:
            return mdDict[type]


    def author_metadata(self, path):
        """
        Stores the current metadata into the path/metadata.json.
        """
        with open(path.joinpath("metadata.json"), "r") as jsonfile:
            jsonfile.write(self.data)



