import orjson as json
from metadata.mdtimeframe import TimeFrame
from pathlib import Path

def write_metadata(mdtype, directory, symbols, timeframe):
    if mdtype == "data":
        data = {}
        data["type"] = "data";
        data["symbols"] = symbols
        data["timeframe"] = timeframe
        dataString = json.dumps(data).decode("utf-8")
        with open(directory.joinpath("metadata.json"), "w") as json_file:
            json_file.write(dataString)
    else:
        print("Metadata type not implemented!")

def read_metadata(mdtype, directory):
    if mdtype == "data":
        with open(directory.joinpath("metadata.json"), "r") as json_file:
            data = json_file.read()
        mdDict = json.loads(data)
        return mdDict
    else:
        print("metadata type not supported")
