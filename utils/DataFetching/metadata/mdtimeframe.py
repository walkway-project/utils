from enum import Enum, unique


@unique
class TimeFrame(Enum):
    TEST = -1  # 6/1/2023 to 7/1/2023
    RECENT = 0  # 9/1/2023 to 4/1/2024
    SHORT = 1  # 1/1/2022 to 7/1/2023
    MED = 2  # 1/1/2020 to 7/1/2023
    LONG = 3  # 1/1/2017 to 7/1/2023
    NEW = 4  # 2/1/2024 to 6/1/2024
    
