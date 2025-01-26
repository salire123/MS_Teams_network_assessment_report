###

# main.py
# This file mainly contains the main code that uses the analyze_data class to analyze the data.
# now it just writeing what should be done in the main code



# Author: salire
# Created: 26-1-2025
# Last Modified: 26-1-2025

#version 0.2

###

import pandas as pd
import matplotlib.pyplot as plt
from utils.data_processing import Data_cleaning_and_preprocessing
from utils.analyze import analyze_data

class DataAnalyzer:
    def __init__(self, data_path: str, output_path: str = "output" ):
        self.data_path = data_path
        self.output_path = output_path
        self.data = None
        self.office_hours = None
        self.non_office_hours = None
        self.analyzed_data = None
        self.list_of_analyzed_columns = ['LossRate-%', 'AverageLatency-Ms', 'AverageJitter-Ms']

    def load_and_preprocess_data(self):
        data_processor = Data_cleaning_and_preprocessing(self.data_path)
        data_processor.get_data()
        self.data = data_processor.data
        self.office_hours, self.non_office_hours = data_processor.daily_office_hours()

    def analyze_data(self):
        if self.office_hours is None:
            raise ValueError("Data must be loaded and preprocessed first. Call load_and_preprocess_data()")

        analyzer = analyze_data(self.office_hours)

        analyze_medians = [analyzer.analyze_every_date_medians(column) for column in self.list_of_analyzed_columns]
        analyze_means = [analyzer.analyze_every_date_means(column) for column in self.list_of_analyzed_columns]
        analyze_95percentiles = [analyzer.analyze_every_date_95percentiles(column) for column in self.list_of_analyzed_columns]

        list_of_analyz_types = ['medians', 'means', '95percentiles']
        list_of_analyz_dataframes = [analyze_medians, analyze_means, analyze_95percentiles]
        final_output = pd.DataFrame()

        for df_num in range(len(list_of_analyz_dataframes)):
            for columns_num in range(len(list_of_analyz_dataframes[df_num])):
                list_of_analyz_dataframes[df_num][columns_num].columns = ['Date', f'{self.list_of_analyzed_columns[columns_num]}_{list_of_analyz_types[df_num]}']
                final_output = pd.concat([final_output, list_of_analyz_dataframes[df_num][columns_num]], axis=1)

        final_output = final_output.loc[:, ~final_output.columns.duplicated()]
        final_output['Date'] = final_output['Date'].astype('string')
        self.analyzed_data = final_output
        return final_output

    def save_output(self, filename_base="final_output"):
        if self.analyzed_data is None:
            raise ValueError("Data must be analyzed first. Call analyze_data()")
        self.analyzed_data.to_excel(f'{self.output_path}/{filename_base}.xlsx', index=False)
        self.analyzed_data.to_csv(f'{self.output_path}/{filename_base}.csv', index=False)

    def plot_data(self):
        if self.analyzed_data is None:
            raise ValueError("Data must be analyzed first. Call analyze_data()")

        for column in self.list_of_analyzed_columns:
            plt.figure()
            plt.plot(self.analyzed_data['Date'], self.analyzed_data[column + '_medians'], label='medians')
            plt.plot(self.analyzed_data['Date'], self.analyzed_data[column + '_means'], label='means')
            plt.plot(self.analyzed_data['Date'], self.analyzed_data[column + '_95percentiles'], label='95percentiles')
            plt.xlabel('Date')
            plt.ylabel(column)
            plt.title(f'{column} for each day')
            plt.legend()
            plt.savefig(f'{self.output_path}/{column}.png')
            plt.close() # Close the figure to prevent memory issues

    def run_analysis(self):
        self.load_and_preprocess_data()
        self.analyze_data()
        self.save_output()
        self.plot_data()


if __name__ == '__main__':
    analyzer = DataAnalyzer('data/172-16-52-20')
    analyzer.run_analysis()
    #Example of accessing the dataframe after the analysis
    #analyzed_df = analyzer.analyzed_data
    #print(analyzed_df.head()) 