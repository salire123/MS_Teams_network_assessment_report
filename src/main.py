###

# main.py
# This file mainly contains the main code that uses the analyze_data class to analyze the data.
# now it just writeing what should be done in the main code



# Author: salire
# Created: 26-1-2025
# Last Modified: 26-1-2025

#version 0.1

###


from utils.data_processing import Data_cleaning_and_preprocessing
from utils.analyze import analyze_data
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == '__main__':
    data = Data_cleaning_and_preprocessing('data/172-16-52-20')
    data.get_data()
    output = data.data
    print(output)
    print(output.dtypes)
    office_hours, non_office_hours = data.daily_office_hours()
    print(office_hours) #list of dataframes
    print(non_office_hours) #list of dataframes


    analyze = analyze_data(office_hours)

    list_of_analyz_cloumns = ['LossRate-%', 'AverageLatency-Ms', 'AverageJitter-Ms']

    analyze_medians = [analyze.analyze_every_date_medians(column_name) for column_name in list_of_analyz_cloumns]
    analyze_means = [analyze.analyze_every_date_means(column_name) for column_name in list_of_analyz_cloumns]
    analyze_95percentiles = [analyze.analyze_every_date_95percentiles(column_name) for column_name in list_of_analyz_cloumns]

    [print(f"Shape of DataFrame {i+1}: {df.shape}\nDataFrame {i+1}:\n{df}\n") for i, df in enumerate(analyze_medians)]
    [print(f"Shape of DataFrame {i+1}: {df.shape}\nDataFrame {i+1}:\n{df}\n") for i, df in enumerate(analyze_means)]
    [print(f"Shape of DataFrame {i+1}: {df.shape}\nDataFrame {i+1}:\n{df}\n") for i, df in enumerate(analyze_95percentiles)]

    list_of_analyz_types = ['medians', 'means', '95percentiles']
    list_of_analyz_dataframes = [analyze_medians, analyze_means, analyze_95percentiles]
    final_output = pd.DataFrame() 
    # combine all the dataframes in the list_of_analyz_dataframes into one dataframe
    # final_output should have the following columns: Date(Timestamp-UTC), LossRate-%_median, LossRate-%_mean, LossRate-%_95percentile, AverageLatency-Ms_median, AverageLatency-Ms_mean, AverageLatency-Ms_95percentile, AverageJitter-Ms_median, AverageJitter-Ms_mean, AverageJitter-Ms_95percentile
    for df_num in range(len(list_of_analyz_dataframes)): # this loop [analyze_medians, analyze_means, analyze_95percentiles]
        print("doing one of the three types of analysis, now in: ", list_of_analyz_types[df_num])
        for columns_num in range(len(list_of_analyz_dataframes[df_num])):
            print("doing one of the three columns, now in: ", list_of_analyz_cloumns[columns_num])
            #rename the columns of the dataframes in the list_of_analyz_dataframes
            list_of_analyz_dataframes[df_num][columns_num].columns = ['Date', f'{list_of_analyz_cloumns[columns_num]}_{list_of_analyz_types[df_num]}']    

            final_output = pd.concat([final_output, list_of_analyz_dataframes[df_num][columns_num]], axis=1)
            
               

    #remove duplicate columns (most likely the Timestamp-UTC column)
    final_output = final_output.loc[:,~final_output.columns.duplicated()]

    print(final_output.dtypes)

    #day to time format
    final_output['Date'] = final_output['Date'].astype('string')

    #save the final_output dataframe to a excel file
    final_output.to_excel('output/final_output.xlsx', index=False)

            
    #plot the data loss rate, average latency and average jitter for each day one plot for each
    for column in list_of_analyz_cloumns:
        plt.figure()
        plt.plot(final_output['Date'], final_output[column+'_medians'], label='medians')
        plt.plot(final_output['Date'], final_output[column+'_means'], label='means')
        plt.plot(final_output['Date'], final_output[column+'_95percentiles'], label='95percentiles')
        plt.xlabel('Date')
        plt.ylabel(column)
        plt.title(f'{column} for each day')
        plt.legend()
        plt.savefig(f'output/{column}.png')

    

        

        
