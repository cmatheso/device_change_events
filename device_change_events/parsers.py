import pandas as pd
import os
import re
from datetime import datetime

EV_PATTERN = re.compile('^ev_dump_.*\.csv$', re.IGNORECASE)
DEVICE_SCHEMA = {
    'timestamp': 'str', 
    'device_id': 'str',
    'event_type': 'str',
    'event_payload': 'str'
}

def __clean_timestamp(rawValue:str) -> datetime:
    try:
        if rawValue is not None:
            actualVal = float(rawValue)
            return datetime.fromtimestamp(actualVal)
    except Exception:
        pass

    return None

def __clean_device_id(rawValue:str) -> str:
    try:
        if rawValue is not None and len(rawValue) == 8:
            # parse as hex into decimal to confirm its a valid value
            _ = int(rawValue, 16)
            return rawValue.lower()
    except Exception:
        pass

    return None

def __clean_event_type(rawValue:str) -> str:
    if rawValue is None or len(rawValue) >= 256:
        # unexpected value, lets skip it
        return None
    
    return rawValue.lower()

def get_ev_data(rootDir:str) -> pd.DataFrame:
    """Retrieves all EV data from the given folder and returns as a DataFrame."""

    combinedDf = None

    for fileName in os.listdir(rootDir):
        absFilePath = os.path.join(rootDir, fileName)
        if not os.path.isfile(absFilePath):
            continue # assuming all files are within the given directory and not subdirectories.

        if not re.match(EV_PATTERN, fileName):
            # not an ev file - skip for now
            continue

        df = pd.read_csv(absFilePath, sep=',', skiprows=1, skip_blank_lines=True, header=None, names=DEVICE_SCHEMA.keys(), dtype=DEVICE_SCHEMA, on_bad_lines='warn')
        df['timestamp'] = df['timestamp'].apply(__clean_timestamp)
        df['device_id'] = df['device_id'].apply(__clean_device_id)
        df['event_type'] = df['event_type'].apply(__clean_event_type)
        df.dropna(inplace=True)

        if combinedDf is None:
            combinedDf = df
        else:
            combinedDf = pd.concat([combinedDf, df])

    return combinedDf
