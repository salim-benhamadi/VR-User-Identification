import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, LabelEncoder

def find_non_varying_variables(df):
    non_varying_columns = []
    variability_percentage = []
    
    for column in df.columns:
        unique_count = df[column].nunique()
        total_count = len(df[column])
        variability = unique_count / total_count * 100
        
        if unique_count == 1:
            non_varying_columns.append(column)
            variability_percentage.append(variability)
    
    result_df = pd.DataFrame({'Variable': non_varying_columns, 'Variability Percentage': variability_percentage})
    return result_df

def missing_columns(dataframe):
    """
    Returns a dataframe that contains missing column names and 
    percent of missing values in relation to the whole dataframe.
    
    dataframe: dataframe that gives the column names and their % of missing values
    """
    
    # find the missing values
    missing_values = dataframe.isnull().sum().sort_values(ascending=False)
    
    # percentage of missing values in relation to the overall size
    missing_values_pct = 100 * missing_values/len(dataframe)
    
    # create a new dataframe which is a concatinated version
    concat_values = pd.concat([missing_values, missing_values/len(dataframe),missing_values_pct.round(1)],axis=1)

    # give new col names
    concat_values.columns = ['Missing Count','Missing Count Ratio','Missing Count %']
    
    # return the required values
    return concat_values[concat_values.iloc[:,1]!=0]

def scaling(df):
    # Select numeric columns only
    numeric_cols = df.select_dtypes(include=['number']).columns.difference(["time_interval"])
    
    # Apply MinMaxScaler to the numeric columns
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(df[numeric_cols])
    
    # Create a DataFrame from the scaled data
    scaled_df = pd.DataFrame(scaled_data, columns=numeric_cols)
    
    # Re-include the string columns back into the DataFrame
    for col in df.columns:
        if col not in numeric_cols:
            scaled_df[col] = df[col].values
            
    return scaled_df


def encoding(df):
    le = LabelEncoder()

    # iterate through all the categorical columns
    for col in df.select_dtypes('object').columns:
        df[col] = le.fit_transform(df[col].astype(str))

    print("Label encoded {col}")
    return df

def match_columns(training_set,testing_set):
    """Matches the count of columns from training set to testing set by adding extra cols and setting them to 0."""
    
    for column in training_set.columns:
        if column not in testing_set.columns:
            testing_set[column]=0
    for column in testing_set.columns:
        if column not in training_set.columns:
            testing_set = testing_set.drop(column)
    return testing_set 

def preprocess(filepaths):
    """
    Process and clean datasets from given filepaths.
    
    Parameters:
    - filepaths: A dictionary with dataset names as keys and filepaths as values.
    
    Returns:
    - A dictionary with dataset names as keys and processed DataFrames as values.
    """
    processed_datasets = {}

    for name, filepath in filepaths.items():
        # Load the dataset
        df = pd.read_csv(filepath)

        # 2.1. Fix Features Naming
        df.columns = df.columns.str.strip()

        # 2.2. Columns Variability 
        non_varying = find_non_varying_variables(df)
        df.drop(columns=non_varying['Variable'], inplace=True)

        # 2.3. Missing Values (Assuming handling missing values is desired but not altering original data)
        # missing_df = missing_columns(df)  # If you want to inspect missing values

        # 2.4. Feature Scaling
        df = scaling(df)

        # 2.5. Label Encoding
        df = encoding(df)

        processed_datasets[name] = df

    # 2.6. Matching Columns - Assuming mov_fast/mov_slow and traffic_fast/traffic_slow pairing
    movement_datasets = ['mov_fast', 'mov_slow']
    traffic_datasets = ['traffic_fast', 'traffic_slow']

    if all(dataset in processed_datasets for dataset in movement_datasets):
        processed_datasets['mov_fast'] = match_columns(processed_datasets['mov_slow'], processed_datasets['mov_fast'])

    if all(dataset in processed_datasets for dataset in traffic_datasets):
        processed_datasets['traffic_fast'] = match_columns(processed_datasets['traffic_slow'], processed_datasets['traffic_fast'])

    # Sort columns for consistency
    for name in processed_datasets:
        processed_datasets[name] = processed_datasets[name].reindex(sorted(processed_datasets[name].columns), axis=1)

    # Optionally save processed datasets
    for name, df in processed_datasets.items():
        df.to_csv(f'../data/processed/{name}_cleaned.csv', index=False)

    return processed_datasets

# Assuming the defined functions find_non_varying_variables, missing_columns, scaling, encoding, and match_columns exist
