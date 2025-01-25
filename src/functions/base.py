###

# base.py
# This file contains the Data_cleaning_and_preprocessing class that is used to clean and preprocess the data.



# Author: salire
# Created: 26-1-2025
# Last Modified: 26-1-2025

###


import os
import pandas as pd
import numpy as np
import glob


class Data_cleaning_and_preprocessing:
    """
    This class is used to clean and preprocess the data.
    folder_path: The path to the folder containing the data files.
    """
    def __init__(self, folder_path->str):
        self.folder_path = folder_path
        self.data = None
        self.datatype = {
        'Timestamp-UTC': 'datetime64[ns]',  # '2024-07-26 10:30:00'
        'LossRate-%': 'float64',  # 0.0 (assuming LossRate-% contains numeric values without '%')
        'AverageLatency-Ms': 'float64',
        'AverageJitter-Ms': 'float64',
        'Protocol': 'string',  # 'UDP'
        'LocalIP': 'string',
        'RemoteIP': 'string',
        'ProxyUsed': 'bool',  # True
        'ReflexiveIP': 'string'
        }

    def old_get_data(self):
        """
        This function reads the data from the CSV files in the folder and combines them into a single DataFrame.
        """
        all_data = []  # List to store individual DataFrames
        for file in os.listdir(self.folder_path):
            if file.endswith('.csv'):
                try:
                    df = pd.read_csv(os.path.join(self.folder_path, file))
                    all_data.append(df)
                except pd.errors.EmptyDataError:
                    print(f'Warning: File {file} is empty and will be skipped.')
                except pd.errors.ParserError:
                    print(f'Warning: File {file} could not be parsed and will be skipped.')
            if all_data:  # check if list is not empty
                self.data = pd.concat(all_data, ignore_index=True)
        
        # change the data type of the columns
        self.data_type()

    def get_data(self):
        try:
            csv_files = glob.glob(os.path.join(self.folder_path, '*.csv'))# get all csv files in the folder
            if not csv_files:
                print('No CSV files found in the folder.')
                return
            
            dfs = (pd.read_csv(file) for file in csv_files)
            self.data = pd.concat(dfs, ignore_index=True, errors='coerce')
            self.data_type()

        except OSError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

    def data_type(self):
        try:
            self.data['Timestamp-UTC'] = pd.to_datetime(self.data['Timestamp-UTC'], format='%Y-%m-%d-%H:%M:%S')
            self.data['LossRate-%'] = self.data['LossRate-%'].astype('float64')
            self.data['AverageLatency-Ms'] = self.data['AverageLatency-Ms'].astype('float64')
            self.data['AverageJitter-Ms'] = self.data['AverageJitter-Ms'].astype('float64')
            self.data['Protocol'] = self.data['Protocol'].astype('string')
            self.data['LocalIP'] = self.data['LocalIP'].astype('string')
            self.data['RemoteIP'] = self.data['RemoteIP'].astype('string')
            self.data['ProxyUsed'] = self.data['ProxyUsed'].astype('bool')
            self.data['ReflexiveIP'] = self.data['ReflexiveIP'].astype('string')

            # Check if the data types are correct
            for column in self.data.columns:
                if self.data[column].dtype != self.datatype[column]:
                    print(f'Warning: Data type of {column} is not correct. Expected {self.datatype[column]} but got {self.data[column].dtype}.')
        except KeyError:
            print('Warning: Data is empty. Please check the data.')
    
    def separate_data(self):
        date_list = self.data['Timestamp-UTC'].dt.date.unique()
        all_data = [] #local variable
        for date in date_list:
            start_of_day = pd.Timestamp(date)
            end_of_day = pd.Timestamp(date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            daily_data = self.data[(self.data['Timestamp-UTC'] >= start_of_day) & (self.data['Timestamp-UTC'] <= end_of_day)]
            if not daily_data.empty:
                all_data.append(daily_data)
        
        if all_data:
            #print(f"Found data for {len(all_data)} days.")
            for i, daily_data in enumerate(all_data):
                date_of_data = daily_data['Timestamp-UTC'].dt.date.iloc[0]
                #print(f"\nData for {date_of_data}:")
                #print(daily_data.head())
        else:
            print("No data found for any days.")
        return all_data #return the list of dataframes
      
    def daily_office_hours(self, start_hour: int = 9, end_hour: int = 18):
        temp_data = self.separate_data()
        office_hours = []
        non_office_hours = []
        for daily_data in temp_data:
            office_hours.append(daily_data[(daily_data['Timestamp-UTC'].dt.hour >= start_hour) & (daily_data['Timestamp-UTC'].dt.hour < end_hour)])
            non_office_hours.append(daily_data[(daily_data['Timestamp-UTC'].dt.hour < start_hour) | (daily_data['Timestamp-UTC'].dt.hour >= end_hour)])
        
        # remove empty dataframes
        office_hours = [df for df in office_hours if not df.empty]
        non_office_hours = [df for df in non_office_hours if not df.empty]
        
        return office_hours, non_office_hours



# test the class
if __name__ == '__main__':
    data = Data_cleaning_and_preprocessing('data/172-16-52-20')
    data.get_data()
    output = data.data
    print(output)
    print(output.dtypes)
    office_hours, non_office_hours = data.daily_office_hours()
    print(office_hours) #list of dataframes
    print(non_office_hours) #list of dataframes

    # Save the office_hours and non_office_hours dataframes to an Excel file
    def save_files_and_lists_to_excel(office_hours, non_office_hours, output_filename):
        # Create a writer object
        writer = pd.ExcelWriter(output_filename)

        # Write each list of dataframes to a separate sheet in the Excel file
        for i, df in enumerate(office_hours):
            df.to_excel(writer, sheet_name=f"Office Hours {i+1}")
        for i, df in enumerate(non_office_hours):
            df.to_excel(writer, sheet_name=f"Non-Office Hours {i+1}")

        # Save the Excel file
        writer._save()

    save_files_and_lists_to_excel(office_hours, non_office_hours, 'output.xlsx')
