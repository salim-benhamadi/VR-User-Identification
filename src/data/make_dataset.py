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
    # Example: Extract IDs from 'group1_order1_user0'
    parts = filepath.split('\\')  # Adjust this based on the actual path separator in your system
    for part in parts:
        if 'group' in part and 'order' in part and 'user' in part:
            return part

def process_movement_data(filepaths):
    """Process movement data and return a DataFrame with statistics."""
    result_stat = pd.DataFrame()

    for filepath in filepaths:
        df = pd.read_csv(filepath)

        df.drop(columns=['time'], inplace=True)
        
        df = df.describe().transpose().drop(columns=['count'])
        
        df = df.stack().to_frame().T
        
        ids = extract_ids(filepath)
        
        df['ID'] = ids

        result_stat = pd.concat([result_stat, df], axis=0)

    # Rename columns with the corresponding statistic and file index
    result_stat.columns = [f'{col}_{stat}' for idx, (col, stat) in enumerate(result_stat.columns)]

    result_stat.reset_index(drop=True, inplace=True)
    return result_stat



@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data into cleaned data."""
    logger = logging.getLogger(__name__)
    logger.info('Making final data set from raw data')

    # Get all files related to the participants' data
    all_files = get_all_files(input_filepath)

    # Separate traffic data and movement data paths
    traffic_data = [x for x in all_files if '_traffic.csv' in x]
    movement_data = [x for x in all_files if '_movement.csv' in x]

    # Process movement data
    movement_fast_stat = process_movement_data([filepath for filepath in movement_data if 'fast' in filepath])
    movement_slow_stat = process_movement_data([filepath for filepath in movement_data if 'slow' in filepath])

    # Save the resulting dataframes
    movement_fast_stat.to_csv(output_filepath + 'movement_fast_stat.csv', index=False)
    movement_slow_stat.to_csv(output_filepath + 'movement_slow_stat.csv', index=False)

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