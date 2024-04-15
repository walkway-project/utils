from catalyst.constants import DEFAULTPATH

import msgpack

import gzip
import os
import concurrent.futures as cf
from pathlib import Path
import copy
import glob
import os
import gzip


class MsgpackParser:
    def __init__(self):
        self.updates = []
        self.trades = []
        self.snapshots = []

        self.update_start_timestamp = None
        self.snapshot_start_timestamp = None
        self.trade_start_timestamp = None

        env_value = os.environ.get("DATA_WAREHOUSE")
        if not env_value:
            self.catalystBase = DEFAULTPATH
            print(f"No $DATA_WAREHOUSE found, using default {self.catalystBase}.")
        else:
            self.catalystBase = Path(env_value)

    def parse_symbol(self, symbol: str):
        # create directories
        os.makedirs(self.catalystBase.joinpath("l2_updates"), exist_ok=True)
        os.makedirs(self.catalystBase.joinpath("trades"), exist_ok=True)
        os.makedirs(self.catalystBase.joinpath("ob_snapshot_50"), exist_ok=True)
        # collect files
        update_files = glob.glob(
            f"{str(self.catalystBase.joinpath(symbol))}/*incremental*"
        )
        trade_files = glob.glob(f"{str(self.catalystBase.joinpath(symbol))}/*trades*")
        snapshot_files = glob.glob(
            f"{str(self.catalystBase.joinpath(symbol))}/*snapshot*"
        )
        with cf.ProcessPoolExecutor() as executor:
            # chunksize determined through benchmarking
            executor.map(self._parse_updates, update_files, chunksize=1)
            executor.map(self._parse_snapshots, snapshot_files, chunksize=3)
            executor.map(self._parse_trades, trade_files, chunksize=1)

    def _parse_updates(self, update_file):
        updates = []
        with gzip.open(update_file) as file:
            next(file)  # skip header
            for line in file:
                # Data Wrangling
                contents = line.decode("utf-8").rstrip("\n").split(",")
                data = []
                data.append(int(contents[2]))
                data.append(False)
                b = False 
                if contents[5] == "bid":
                    b = True
                data.append(b)
                data.append(float(contents[6]))
                data.append(float(contents[7]))

                updates.append(copy.deepcopy(data))
            msgpack.dump(
                updates,
                open(
                    self.catalystBase.joinpath("l2_updates").joinpath(
                        f"{update_file.split('/')[-1].split('.')[0].split('_')[4]}.mpk"
                    ),
                    "wb+",
                ),
            )

    def _parse_snapshots(self, snapshot_file):
        snapshots = []
        with gzip.open(snapshot_file) as file:
            next(file)
            for line in file:
                contents = line.decode("utf-8").rstrip("\n").split(",")
                data = []
                data.append(int(contents[2]))
                data.append([])
                data.append([])
                data.append([])
                data.append([])
                contents = contents[4:]  # remove exchange, symbol, timestamp, local timestamp
                index = 1
                for i in range(0, len(contents), 2):
                    if i % 4 == 0:
                        index += 1
                        data[3].append(float(contents[i]))
                        data[4].append(float(contents[i + 1]))
                    else:
                        data[1].append(float(contents[i]))
                        data[2].append(float(contents[i + 1]))
                snapshots.append(copy.deepcopy(data))
            msgpack.dump(
                snapshots,
                open(
                    self.catalystBase.joinpath("ob_snapshot_50").joinpath(
                        f"snapshot{snapshot_file.split('/')[-1].split('.')[0].split('_')[4]}.mpk"
                    ),
                    "wb+",
                ),
            )

    def _parse_trades(self, trade_file):
        with gzip.open(trade_file) as file:
            next(file)  # Skip header
            for line in file:
                contents = line.decode("utf-8").rstrip("\n").split(",")
                data = []
                data.append(int(contents[2]))
                data.append(False)
                if contents[5] == "buy":
                    data[-1] = True
                data.append(float(contents[6]))
                data.append(float(contents[7]))
                data.append(1)
                self.trades.append(copy.deepcopy(data))

            msgpack.dump(
                self.trades,
                open(
                    self.catalystBase.joinpath("trades").joinpath(
                        f"trades{trade_file.split('/')[-1].split('.')[0].split('_')[4]}.mpk"
                    ),
                    "wb+",
                ),
            )
            self.trades.clear()


if __name__ == "__main__":
    parser = MsgpackParser()
    parser.parse_symbol("XRPUSDT")
