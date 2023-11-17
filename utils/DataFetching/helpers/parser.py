import gzip
import os
import shutil

def extract_string(input_string):
    ftype = "snapshots"
    if "trades" in input_string:
        ftype = "trades"
    elif "increment" in input_string:
        ftype = "updates"
    base_name = input_string.split(".")[0]
    substring = base_name.split("_")
    time = substring[-2]
    substring = substring[-1]
    return ftype, time, substring

ftype_features = {
    "snapshots" : "ob_snapshot_50",
    "trades" : "trades",
    "updates" : "l2_updates" 
}

def parse_updates(source, updatesFile, stop_at=None):
    i = 0
    updatesFile.write("pair|side|time|price|volume|is_snapshot\n")
    next(source)  # skip header
    for line in source:
        i += 1
        if stop_at and i > stop_at:
            break
        contents = line.decode("utf-8").rstrip("\n").split(",")
        if contents == [""]:
            continue
        content = {  # for readability
            "product_id": contents[1],
            "side": contents[5],
            "time": contents[2],
            "price": contents[6],
            "size": contents[7],
            "is_snapshot":contents[4],
        }
        updatesFile.write(
            f"{content['product_id']}|{content['side']}|{content['time']}|{content['price']}|{content['size']}|{content['is_snapshot']}\n"
        )


def parse_snapshots(source, snapshotsFile, stop_at=None):
    i = 0
    snapshotsFile.write("pair|time|bids|asks\n")
    next(source)  # skip header
    for line in source:
        contents = line.decode("utf-8").rstrip("\n").split(",")
        if contents == [""]:
            continue
        bids, asks, content = {}, {}, {}
        content["pair"] = contents[1]
        content["time"] = contents[2]
        contents = contents[4:]
        content["bids"] = []
        content["asks"] = []
        for i in range(0, len(contents), 2):
            if i % 4 == 0:
                content["asks"].append([float(contents[i]), float(contents[i + 1])])
            else:
                content["bids"].append([float(contents[i]), float(contents[i + 1])])
        snapshotsFile.write(
            f"{content['pair']}|{content['time']}|{content['bids']}|{content['asks']}\n"
        )
        i += 1
        if stop_at and i > stop_at:
            break


def parse_trades(source, tradesFile, stop_at=None):
    i = 0
    next(source)  # skip header
    tradesFile.write("pair|side|time|price|volume|trade_id\n")
    for line in source:
        i += 1
        if stop_at and i > stop_at:
            break
        contents = line.decode("utf-8").rstrip("\n").split(",")
        if contents == [""]:
            continue
        content = {  # for readability
            "product_id": contents[1],
            "side": contents[5],
            "time": contents[2],
            "price": contents[6],
            "size": contents[7],
            "trade_id": contents[4],
        }
        tradesFile.write(
            f"{content['product_id']}|{content['side']}|{content['time']}|{content['price']}|{content['size']}|{content['trade_id']}\n"
        )

def process_download(symbol, catalystBase):
    base = catalystBase
    directories = [symbol]
    directories = [str(base / entry) for entry in directories]
    snapcount = 0
    for pair_dir in directories:
        files = os.listdir(pair_dir)
        files = sorted([f for f in files if f.endswith(".gz")])
        for f in files: #for source gzip file
            ftype, time, pair = extract_string(f)
            feature_target = str(base/ftype_features[ftype])
            if not os.path.exists(feature_target):
                os.makedirs(feature_target)
            target_dir = str(base / ftype_features[ftype] / pair) #this is where it is going:
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            targetfile = target_dir + "/" + time + ".csv"
            with gzip.open(pair_dir + "/" + f, "r") as f_in:
                with open(targetfile, "w+") as f_out:
                    try:
                        if ftype == "updates":
                            parse_updates(f_in, f_out)
                        elif ftype == "trades":
                            parse_trades(f_in, f_out)
                        else:
                            if snapcount == 0:
                                # NOTE: put stop at to 1 to parse the first snapshot only
                                parse_snapshots(f_in, f_out, stop_at=None)
                                # snapcount += 1
                    except Exception as e:
                        print(f"Error parsing {pair_dir}/{f}")
                        print(e)
                        continue
        shutil.rmtree(pair_dir)
