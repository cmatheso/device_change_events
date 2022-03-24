
from typing import Tuple
import pandas as pd
import numpy as np

def agg_ev_events_per_hour(evDf: pd.DataFrame) -> pd.DataFrame:
    """
    Generates the min, max, and mean of the count of each event type, per device_id, per hour.
    """

    # Create rollup columns:
    aggDf = evDf.copy()
    aggDf['date'] = aggDf['timestamp'].dt.date
    aggDf['hh'] = aggDf['timestamp'].dt.strftime('%H')
    aggDf['mm'] = aggDf['timestamp'].dt.strftime('%M')
    aggDf['ss'] = aggDf['timestamp'].dt.strftime('%S')

    # Lets create the base aggregation at the minutes level
    aggDf = aggDf.groupby(['date', 'hh', 'mm', 'device_id', 'event_type']).agg(count_of_events=pd.NamedAgg(column='event_payload', aggfunc='count'))

    # Rollup to hours
    aggDf = aggDf.groupby(['date', 'hh', 'device_id', 'event_type']) \
        .agg(
            min_events_per_min=pd.NamedAgg(column='count_of_events', aggfunc='min'),
            max_events_per_min=pd.NamedAgg(column='count_of_events', aggfunc='max'),
            avg_events_per_min=pd.NamedAgg(column='count_of_events', aggfunc='mean')
        )

    return aggDf

def agg_ev_events_histogram(evDf: pd.DataFrame, eventType:str, bins:int = 10) -> Tuple[pd.DataFrame, list[int], list[int]]:
    """
    Generates histogram data for a given eventType.
    """

    # Pull out just the required event_type
    aggDf = evDf.copy()
    aggDf = aggDf[aggDf['event_type'] == eventType]
    aggDf = aggDf.groupby(['device_id']).agg(count_of_events=pd.NamedAgg(column='event_payload', aggfunc='count'))

    count, division = np.histogram(aggDf['count_of_events'], bins=bins)
    return aggDf, count.data.tolist(), division.data.tolist()
