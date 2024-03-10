# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd
import numpy as np
import os
from scipy.spatial.distance import euclidean

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

def feature_engineering(df, data_type):
    if data_type=="movement":
        # Calculate time deltas
        df['Δt'] = df['time'].diff().shift(-1)  
        df.loc[df.index[-1], 'Δt'] = df['Δt'].iloc[-2]
        
        segments = ['Head', 'LeftTouch', 'RightTouch']
        
        for segment in segments:
            df[f'Velocity_{segment}PosX'] = df[f'{segment}PosX'].diff() / df['Δt']
            df[f'Velocity_{segment}PosY'] = df[f'{segment}PosY'].diff() / df['Δt']
            df[f'Velocity_{segment}PosZ'] = df[f'{segment}PosZ'].diff() / df['Δt']
            df[f'{segment}_Velocity'] = np.sqrt(sum(df[col]**2 for col in [f'Velocity_{segment}PosX', f'Velocity_{segment}PosY',f'Velocity_{segment}PosZ']))


            df[f'Accel_{segment}PosX'] = df[f'Velocity_{segment}PosX'].diff() / df['Δt']
            df[f'Accel_{segment}PosY'] = df[f'Velocity_{segment}PosY'].diff() / df['Δt']
            df[f'Accel_{segment}PosZ'] = df[f'Velocity_{segment}PosZ'].diff() / df['Δt']

            df[f'{segment}_OrientationVelocityX'] = df[f'{segment}OrientationX'].diff() / df['Δt']
            df[f'{segment}_OrientationVelocityY'] = df[f'{segment}OrientationY'].diff() / df['Δt']
            df[f'{segment}_OrientationVelocityZ'] = df[f'{segment}OrientationZ'].diff() / df['Δt']

            df[f'{segment}_OrientationAccelX'] = df[f'{segment}_OrientationVelocityX'].diff() / df['Δt']
            df[f'{segment}_OrientationAccelY'] = df[f'{segment}_OrientationVelocityY'].diff() / df['Δt']
            df[f'{segment}_OrientationAccelZ'] = df[f'{segment}_OrientationVelocityZ'].diff() / df['Δt']

        
        
        # Relative Positioning and Distance Features
        for pair in [('LeftTouch', 'Head'), ('RightTouch', 'Head'), ('LeftTouch', 'RightTouch')]:
            segment1, segment2 = pair
            # Calculate Euclidean distance between pairs of segments for each row
            df[f'distance_{segment1}_to_{segment2}'] = df.apply(lambda row: euclidean(
                (row[f'{segment1}PosX'], row[f'{segment1}PosY'], row[f'{segment1}PosZ']),
                (row[f'{segment2}PosX'], row[f'{segment2}PosY'], row[f'{segment2}PosZ'])
            ), axis=1)
    if data_type=="traffic":
        df['Δt'] = df['time'].diff().fillna(0) 

        # Traffic Flow Features
        df['size_cumsum'] = df.groupby('direction')['size'].cumsum()
        df['size_rate'] = df['size'] / df['Δt'].replace({0: np.nan})
        df['packet_count'] = df.groupby('direction').cumcount() + 1

    # Drop the temporary Δt column
    df.drop(columns=["Δt"], inplace=True)
    return df

        


def process_data(filepaths, time_window, data_type):
    result_stat = pd.DataFrame()

    for filepath in filepaths:
        df = pd.read_csv(filepath)
        df = feature_engineering(df, data_type)

        df['time_interval'] = (df['time'] / 10).astype(int)
        max_interval = time_window * 6
        df = df[df['time_interval'] < max_interval]

        interval_stats = pd.DataFrame()

        for interval in range(max_interval):
            interval_df = df[df['time_interval'] == interval].drop(columns=['time', 'time_interval'])
            if interval_df.empty:
                continue

            num_cols = interval_df.select_dtypes(include=['number']).columns
            cat_cols = interval_df.select_dtypes(exclude=['number']).columns

            stats = pd.DataFrame()

            if not num_cols.empty:
                num_stats = interval_df[num_cols].describe().transpose()
                num_stats['skew'] = interval_df[num_cols].skew()
                num_stats['kurtosis'] = interval_df[num_cols].kurtosis()
                stats = pd.concat([stats, num_stats.drop(columns=['count']).stack().to_frame().T], axis=1)

            if not cat_cols.empty:
                cat_stats = pd.DataFrame(index=[0])  # Ensure a single-row DataFrame for concatenation
                for col in cat_cols:
                    # Calculate mode and handle if the mode series is empty
                    mode_series = interval_df[col].mode()
                    if mode_series.empty:
                        mode_val = np.nan
                    else:
                        mode_val = mode_series[0]

                    # Add calculated statistics to the cat_stats DataFrame
                    cat_stats[col + '_mode'] = [mode_val]  # Mode value

                    # Count each category and add as separate columns
                    category_counts = interval_df[col].value_counts()
                    for category, count in category_counts.items():
                        # Format the column name to include the category name, making it URL-friendly
                        category_name = str(category).replace(" ", "_").replace("/", "_").lower()
                        cat_stats[f'{col}_count_{category_name}'] = count

                stats = pd.concat([stats, cat_stats], axis=1)


            if stats.empty:
                continue

            ids = extract_ids(filepath)
            stats['time_interval'] =int(interval/ 6) + 1 
            stats['ID'] = ids

            interval_stats = pd.concat([interval_stats, stats], ignore_index=True)

        result_stat = pd.concat([result_stat, interval_stats], ignore_index=True)

    new_columns = ['_'.join(col) if isinstance(col, tuple) else col for col in result_stat.columns]
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

    # # # Process movement data
    logger.info('Processing Fast Movement Data')
    movement_fast_stat = process_data([filepath for filepath in movement_data if 'fast' in filepath],time_window,"movement")
    logger.info('Processing Slow Movement Data')
    movement_slow_stat = process_data([filepath for filepath in movement_data if 'slow' in filepath], time_window,"movement")

    # # Process traffic data
    logger.info('Processing Fast Traffic Data')
    traffic_fast_stat = process_data([filepath for filepath in traffic_data if 'fast' in filepath], time_window,"traffic")
    logger.info('Processing Slow Traffic Data')
    traffic_slow_stat = process_data([filepath for filepath in traffic_data if 'slow' in filepath], time_window,"traffic")

    # # Save the resulting dataframes
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