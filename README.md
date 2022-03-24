# device_change_events

## Environment Setup:
- Requires Python 3.10+.
- Recommend using VSCode for development.
- Create a new venv:
    python -m venv .venv
- Activate venv.
- Install requirements:
    Dev / Sandbox usage: 
        pip install -r dev-requirements.txt
    CLI app only:
        pip install -r requirements.txt

## To execute:
python main.py --inputDir 'sample-data' --outputDir 'output'

## Outputs:
Expecting to see 2 output files upon execution:
1) ev_events_per_hour.csv -> This file contains hourly aggregation data with the count of ev events, per event_type, per device_id, per minute. Note: This data only represents the data that was received, and not the lack of data (eg. min could in fact be 0).
2) ev_squirrel_hist_data.json -> This file contains histogram data including the distribution of counts of squirrel event_type by device, and the bin boundaries serialized into json due to its unstructured form.

## To extend:
- Parsing functionality can be found under the device_change_events/parsers.py file.
- Aggregation functionality can be found under the device_change_events/aggregators.py file.
- main.py is the primary driver for the CLI app. Additional aggregations can be added towards the end of the file as needed. The source parsed data is shared amongst the aggregations and can be easily reused.
- For data exploration and debugging, recommend using the juypter notebook under sandbox.ipynb.