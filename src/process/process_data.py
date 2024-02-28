# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
@click.argument('time_window', type=int)
def main(input_filepath, output_filepath, time_window = 10):
    """Runs data processing scripts to turn raw data into cleaned data."""
    logger = logging.getLogger(__name__)
    logger.info('Making final dataset from raw data')

    pass

    # Save the resulting dataframes
    movement_fast_stat.to_csv(output_filepath + 'movement_fast_stat_cleaned.csv', index=False)
    movement_slow_stat.to_csv(output_filepath + 'movement_slow_stat_cleaned.csv', index=False)
    traffic_fast_stat.to_csv(output_filepath + 'traffic_fast_stat_cleaned.csv', index=False)
    traffic_slow_stat.to_csv(output_filepath + 'traffic_slow_stat_cleaned.csv', index=False)


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