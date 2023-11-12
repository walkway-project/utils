from enum import Enum

class TimeFrame(Enum):
    TEST = -1 #6/1/2023 to 7/1/2023
    SHORT = 1 #1/1/2022 to 7/1/2023
    MED = 2 #1/1/2020 to 7/1/2023
    LONG = 3 #1/1/2017 to 7/1/2023

def timeframe_lookup(val:int):
    """Given an input, returns the corresponding TimeFrame object. If it does not exist, this will throw an error."""
    return next(member for member in TimeFrame if member.value == val)