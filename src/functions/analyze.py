###

# analyze.py
# This file contains the analyze_data class that is used to analyze the data.



# Author: salire
# Created: 26-1-2025
# Last Modified: 26-1-2025

###

import pandas as pd
import numpy as np
import os
from base import Data_cleaning_and_preprocessing

#LossRate-%,AverageLatency-Ms,AverageJitter-Ms
class analyze_data:
    def __init__(self, data: pd.DataFrame):
        self.data = data #list of dataframes

    def analyze_every_date_medians(self, column_name: str):
        analyze_medians = pd.DataFrame(columns=['Timestamp-UTC', column_name])
        for df in self.data:
            # Empty DataFrame
            if self.data is not None:
                temp = pd.DataFrame(columns=['Timestamp-UTC', column_name])
                temp = df.groupby(df['Timestamp-UTC'].dt.date)[column_name].median().reset_index()
                analyze_medians = analyze_medians._append(temp, ignore_index=True)

        return analyze_medians
    
    def analyze_every_date_means(self, column_name: str):
        analyze_means = pd.DataFrame(columns=['Timestamp-UTC', column_name])
        for df in self.data:
            # Empty DataFrame
            if self.data is not None:
                temp = pd.DataFrame(columns=['Timestamp-UTC', column_name])
                temp = df.groupby(df['Timestamp-UTC'].dt.date)[column_name].mean().reset_index()
                analyze_means = analyze_means._append(temp, ignore_index=True)

        return analyze_means
    
    def analyze_every_date_95percentiles(self, column_name: str):
        analyze_95percentiles = pd.DataFrame(columns=['Timestamp-UTC', column_name])
        for df in self.data:
            # Empty DataFrame
            if self.data is not None:
                temp = pd.DataFrame(columns=['Timestamp-UTC', column_name])
                temp = df.groupby(df['Timestamp-UTC'].dt.date)[column_name].quantile(0.95).reset_index()
                analyze_95percentiles = analyze_95percentiles._append(temp, ignore_index=True)

        return analyze_95percentiles

# test the code
if __name__ == '__main__':
    data = Data_cleaning_and_preprocessing('data/172-16-52-20')
    data.get_data()
    office_hours, non_office_hours = data.daily_office_hours()
    office_hours_analyzer = analyze_data(office_hours)
    non_office_hours_analyzer = analyze_data(non_office_hours)

    print("Office Hours:")
    print(office_hours_analyzer.analyze_every_date_medians('AverageJitter-Ms'))
    print(office_hours_analyzer.analyze_every_date_means('AverageJitter-Ms'))
    print(office_hours_analyzer.analyze_every_date_95percentiles('AverageJitter-Ms'))