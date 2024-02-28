# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd
import os


def get_all_files(data_path):
    """Retrieve all files in the specified directory."""
    all_files = []
    for path, _, files in os.walk(data_path):
        all_files.extend([os.path.join(path, name) for name in files])
    return all_files


def extract_ids(filepath):
    """Extract IDs from the file path."""
    parts = filepath.split('\\') 
    for part in parts:
        if 'group' in part and 'order' in part and 'user' in part:
            return part

def process_data(filepaths, time_window):
    """Process CSV data and return a DataFrame with statistics for each 10-second window within the specified time window in minutes."""
    result_stat = pd.DataFrame()

    for filepath in filepaths:
        df = pd.read_csv(filepath)

        # Convert 'time' from seconds to 10-second intervals (1/6 of a minute) and filter based on time_window in minutes
        df['time_interval'] = (df['time'] / 10).astype(int)
        max_interval = time_window * 6  # Convert time_window from minutes to 10-second intervals
        df = df[df['time_interval'] < max_interval]

        # Initialize a DataFrame to store statistics for each 10-second window
        interval_stats = pd.DataFrame()

        for interval in range(max_interval):
            # Filter data for the current 10-second interval
            if interval not in df.time_interval.unique():
                continue
            interval_df = df[df['time_interval'] == interval].drop(columns=['time_interval'])
            if interval_df.empty:
                continue  # Skip this loop iteration if no data for the current interval
            
            # Compute statistics for the current interval
            stats = interval_df.describe().transpose().drop(columns=['count'])
            stats = stats.stack().to_frame().T

            # Add a column for the interval and ID
            ids = extract_ids(filepath)  # Assuming extract_ids function exists
            stats['time_interval'] = interval
            stats['ID'] = ids

            # Append the stats of the current interval
            interval_stats = pd.concat([interval_stats, stats], axis=0)

        # Concatenate the statistics of the current file to the final result
        result_stat = pd.concat([result_stat, interval_stats], axis=0)

    # Rename columns with the corresponding statistic and interval index
    new_columns = [f'{col}_{stat}' if col not in ['time_interval', 'ID'] else col for col, stat in result_stat.columns]
    result_stat.columns = new_columns

    result_stat.reset_index(drop=True, inplace=True)
    return result_stat



@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
@click.argument('time_window', type=int)
def main(input_filepath, output_filepath, time_window = 10):
    """Runs data processing scripts to extract features from raw data."""
    logger = logging.getLogger(__name__)
    logger.info('Making final statistical summary dataset from raw data')

    # Get all files related to the participants' data
    all_files = get_all_files(input_filepath)

    # Separate traffic data and movement data paths
    traffic_data = [x for x in all_files if '_traffic.csv' in x]
    movement_data = [x for x in all_files if '_movement.csv' in x]

    # Process movement data
    movement_fast_stat = process_data([filepath for filepath in movement_data if 'fast' in filepath],time_window)
    movement_slow_stat = process_data([filepath for filepath in movement_data if 'slow' in filepath], time_window)

    # Process traffic data
    traffic_fast_stat = process_data([filepath for filepath in traffic_data if 'fast' in filepath], time_window)
    traffic_slow_stat = process_data([filepath for filepath in traffic_data if 'slow' in filepath], time_window)

    # Save the resulting dataframes
    movement_fast_stat.to_csv(output_filepath + 'movement_fast_stat.csv', index=False)
    movement_slow_stat.to_csv(output_filepath + 'movement_slow_stat.csv', index=False)
    traffic_fast_stat.to_csv(output_filepath + 'traffic_fast_stat.csv', index=False)
    traffic_slow_stat.to_csv(output_filepath + 'traffic_slow_stat.csv', index=False)


    logger.info('Save processed data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # Find .env automagically by walking up directories until it's found,
    # then load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()