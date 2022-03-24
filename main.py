import argparse
import os
import json
import device_change_events.parsers as p
import device_change_events.aggregators as a

argParser = argparse.ArgumentParser(description='A processing library for device_change_events.')
argParser.add_argument('--inputDir', help='The input directory to process.')
argParser.add_argument('--outputDir', help='The output directory to write to.')
args = argParser.parse_args()

if not os.path.exists(args.inputDir):
    raise Exception('Input directory not valid: ' + args.inputDir)

if not args.outputDir or len(args.outputDir) == 0:
    raise Exception('Invalid output directory. Please specify a valid output folder.')


evDf = p.get_ev_data(args.inputDir)

# Prepare output directory:
if not os.path.exists(args.outputDir):
    os.makedirs(args.outputDir)

# Contents for agg 1:
aggDf1 = a.agg_ev_events_per_hour(evDf)
aggDf1.to_csv(os.path.join(args.outputDir, 'ev_events_per_hour.csv'), date_format='%Y-%m-%d') # escapechar="\"", quotechar="\"",

# Contents for agg 2:
aggDf2, count, division = a.agg_ev_events_histogram(evDf, 'squirrel')
with open(os.path.join(args.outputDir, 'ev_squirrel_hist_data.json'), "w") as f:
    data = {
        'hist_device_count': count,
        'hist_boundaries': division
    }

    f.write(json.dumps(data))
