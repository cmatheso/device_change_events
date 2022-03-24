
import pandas as pd
import os

def agg_ev_events_per_hour(evDf: pd.DataFrame, outputDir:str):
    """
    Generates the min, max, and mean of the count of each event type, per device_id, per hour.
    """

    # Create rollup columns:
    evDf['date'] = evDf['timestamp'].dt.date
    evDf['hh'] = evDf['timestamp'].dt.strftime('%H')
    evDf['mm'] = evDf['timestamp'].dt.strftime('%M')
    evDf['ss'] = evDf['timestamp'].dt.strftime('%S')

    # Lets create the base aggregation at the minutes level
    evDf = evDf.groupby(['date', 'hh', 'mm', 'device_id', 'event_type']).agg(count_of_events=pd.NamedAgg(column='event_payload', aggfunc='count'))

    # Rollup to hours
    minutesEvDf = evDf.groupby(['date', 'hh', 'device_id', 'event_type']) \
        .agg(
            min_events_per_min=pd.NamedAgg(column='count_of_events', aggfunc='min'),
            max_events_per_min=pd.NamedAgg(column='count_of_events', aggfunc='max'),
            avg_events_per_min=pd.NamedAgg(column='count_of_events', aggfunc='mean')
        )

    # display(minutesevDf)

    fileName = os.path.join(outputDir, 'ev_events_per_hour.csv')
    if not os.path.exists(os.path.dirname(fileName)):
        os.makedirs(os.path.dirname(fileName))

    minutesEvDf.to_csv(fileName, date_format='%Y-%m-%d') # escapechar="\"", quotechar="\"",
