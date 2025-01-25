#path:

import pandas as pd
import os
import pytz
import matplotlib.pyplot as plt
import shutil
from fun import data_DataProcess
class do_main():
    def __init__(self, savepath, source_path):
        self.savepath = savepath
        self.source_path = source_path
        
    def run_main(self):
        savepath = self.savepath
        source_path = self.source_path

        if os.path.exists(savepath):
            shutil.rmtree(savepath)

        #if savepath not exist, create it
        if not os.path.exists(savepath):
            os.makedirs(savepath)
            os.makedirs(savepath+'office')
            os.makedirs(savepath+'non_office')

            os.chmod(savepath, 0o777)
            os.chmod(savepath+'office', 0o777)
            os.chmod(savepath+'non_office', 0o777)


        if os.path.isdir(source_path):
            df = pd.DataFrame() 
            for f in os.listdir(source_path):
                try:
                    df = pd.concat([df, pd.read_csv(os.path.join(source_path, f))])
                    print(df)
                except:
                    print('error file: ', f)
                    error_file = os.path.join(savepath, 'error_file.txt')
                    with open(error_file, 'a') as f_out:
                        f_out.write(f + '\n')
            combined_csv = os.path.join(savepath, 'combined.csv')
            df.to_csv(combined_csv, index=False)

        try:
            df = pd.read_csv(combined_csv, parse_dates=['Timestamp-UTC']) 
        except PermissionError as e:
            print("Permission error: ", e)
            exit(1)

        #make the csv file to use UTC+8 time zone and delete the row of 'LossRate-%' is 100
        tz = pytz.timezone('Asia/Shanghai') 
        df['Timestamp-UTC'] = df['Timestamp-UTC'].dt.tz_localize('UTC')
        df['Timestamp-UTC'] = df['Timestamp-UTC'].dt.tz_convert(tz)
        df = df[df['LossRate-%'] != 100]

        df.to_csv(savepath+'combine_utc8.csv')

        grouped = df.groupby(df['Timestamp-UTC'].dt.date)

        for dateset, group in grouped:
            #get each day's data office hour and non-office hour
            office_hour, non_office_hour = data_DataProcess.get_daily_data(group)
            office_hour.to_csv(savepath+'office/office_hour_'+str(dateset)+'.csv')
            non_office_hour.to_csv(savepath+'non_office/non_office_hour_'+str(dateset)+'.csv')

        median_office_median, median_non_office_median = data_DataProcess.get_each_day_median(df)
        #to csv
        median_office_median.to_csv(savepath+'median_office_median.csv')
        median_non_office_median.to_csv(savepath+'median_non_office_median.csv')

        average_office_average, average_non_office_average = data_DataProcess.get_each_day_average(df)
        #to csv
        average_office_average.to_csv(savepath+'average_office_average.csv')
        average_non_office_average.to_csv(savepath+'average_non_office_average.csv')

        pencentage_office, pencentage_non_office = data_DataProcess.get_percentage(df)
        #to csv
        pencentage_office.to_csv(savepath+'pencentage_office.csv')
        pencentage_non_office.to_csv(savepath+'pencentage_non_office.csv')





        def visualize_median_average(median_office_median, median_non_office_median, average_office_average, average_non_office_average, pencentage_office, pencentage_non_office):
            r'''
            visualize the median and average
            '''
            #median
            plt.figure(figsize=(20, 10))
            plt.plot(median_office_median['Timestamp-UTC'], median_office_median['Office Loss Rate Median'], label='Office Loss Rate Median')
            plt.plot(median_office_median['Timestamp-UTC'], median_office_median['Office Latency Median'], label='Office Latency Median')
            plt.plot(median_office_median['Timestamp-UTC'], median_office_median['Office Jitter Median'], label='Office Jitter Median')
            plt.plot(median_non_office_median['Timestamp-UTC'], median_non_office_median['Non-Office Loss Rate Median'], label='Non-Office Loss Rate Median')   
            plt.plot(median_non_office_median['Timestamp-UTC'], median_non_office_median['Non-Office Latency Median'], label='Non-Office Latency Median')
            plt.plot(median_non_office_median['Timestamp-UTC'], median_non_office_median['Non-Office Jitter Median'], label='Non-Office Jitter Median')
            plt.legend()
            plt.title('Median of Office Hour and Non-Office Hour')
            plt.xlabel('Date')
            plt.ylabel('Median')
            plt.savefig(savepath+'median.png')
            plt.close()

            #average
            plt.figure(figsize=(20, 10))
            plt.plot(average_office_average['Timestamp-UTC'], average_office_average['Office Loss Rate Average'], label='Office Loss Rate Average')
            plt.plot(average_office_average['Timestamp-UTC'], average_office_average['Office Latency Average'], label='Office Latency Average')
            plt.plot(average_office_average['Timestamp-UTC'], average_office_average['Office Jitter Average'], label='Office Jitter Average')
            plt.plot(average_non_office_average['Timestamp-UTC'], average_non_office_average['Non-Office Loss Rate Average'], label='Non-Office Loss Rate Average')
            plt.plot(average_non_office_average['Timestamp-UTC'], average_non_office_average['Non-Office Latency Average'], label='Non-Office Latency Average')
            plt.plot(average_non_office_average['Timestamp-UTC'], average_non_office_average['Non-Office Jitter Average'], label='Non-Office Jitter Average')
            plt.legend()
            plt.title('Average of Office Hour and Non-Office Hour')
            plt.xlabel('Date')
            plt.ylabel('Average')
            plt.savefig(savepath+'average.png')
            plt.close()

            #percentage
            plt.figure(figsize=(20, 10))
            plt.plot(pencentage_office['Timestamp-UTC'], pencentage_office['Office Loss Rate 95%'], label='Office Loss Rate Percentile 95%')
            plt.plot(pencentage_office['Timestamp-UTC'], pencentage_office['Office Latency 95%'], label='Office Latency Percentile 95%')
            plt.plot(pencentage_office['Timestamp-UTC'], pencentage_office['Office Jitter 95%'], label='Office Jitter Percentile 95%')
            plt.plot(pencentage_non_office['Timestamp-UTC'], pencentage_non_office['Non-Office Loss Rate 95%'], label='Non-Office Loss Rate Percentile 95%')
            plt.plot(pencentage_non_office['Timestamp-UTC'], pencentage_non_office['Non-Office Latency 95%'], label='Non-Office Latency Percentile 95%')
            plt.plot(pencentage_non_office['Timestamp-UTC'], pencentage_non_office['Non-Office Jitter 95%'], label='Non-Office Jitter Percentile 95%')
            plt.legend()
            plt.title('Percentage of Office Hour and Non-Office Hour')
            plt.xlabel('Date')
            plt.ylabel('Percentage')
            plt.savefig(savepath+'percentage.png')
            plt.close()    

        visualize_median_average(median_office_median, median_non_office_median, average_office_average, average_non_office_average, pencentage_office, pencentage_non_office)