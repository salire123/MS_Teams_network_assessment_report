import pandas as pd


class data_DataProcess:

    def get_daily_data(group):
        office_hour = group[group['Timestamp-UTC'].dt.hour.between(9, 17)]
        non_office_hour = group[~group['Timestamp-UTC'].dt.hour.between(9, 17)]
        return office_hour, non_office_hour

    def get_each_day_median(df):
        r'''
        get the median of each day
        '''
        daily_office_median = pd.DataFrame()
        daily_non_office_median = pd.DataFrame()

        grouped = df.groupby(df['Timestamp-UTC'].dt.date)

        for dateset, group in grouped:
            #get each day's data office hour and non-office hour
            office_hour, non_office_hour = data_DataProcess.get_daily_data(group)

            
            #median
            numeric_cols = ['LossRate-%', 'AverageLatency-Ms', 'AverageJitter-Ms']
            office_oneday_median = office_hour.groupby(office_hour['Timestamp-UTC'].dt.date)[numeric_cols].median()
            non_office_oneday_median = non_office_hour.groupby(non_office_hour['Timestamp-UTC'].dt.date)[numeric_cols].median()

            #add the date column
            office_oneday_median['Timestamp-UTC'] = dateset
            non_office_oneday_median['Timestamp-UTC'] = dateset

            #rename the columns
            office_oneday_median.rename(columns={'LossRate-%':'Office Loss Rate Median', 
                                    'AverageLatency-Ms':'Office Latency Median',
                                    'AverageJitter-Ms':'Office Jitter Median'}, 
                        inplace=True)

            non_office_oneday_median.rename(columns={'LossRate-%':'Non-Office Loss Rate Median', 
                                            'AverageLatency-Ms':'Non-Office Latency Median',
                                            'AverageJitter-Ms':'Non-Office Jitter Median'},
                                    inplace=True)
            
            #daily office median and non-office median if not exist, create it
            #if exist, append the data
            
            daily_office_median = pd.concat([daily_office_median, office_oneday_median], ignore_index=True)
            daily_non_office_median = pd.concat([daily_non_office_median, non_office_oneday_median], ignore_index=True)

        return daily_office_median, daily_non_office_median

    def get_each_day_average(df):

        
        r'''
        get the average of each day
        '''
        daily_office_average = pd.DataFrame()
        daily_non_office_average = pd.DataFrame()

        grouped = df.groupby(df['Timestamp-UTC'].dt.date)

        for dateset, group in grouped:
            office_hour, non_office_hour = data_DataProcess.get_daily_data(group)

            #average
            numeric_cols = ['LossRate-%', 'AverageLatency-Ms', 'AverageJitter-Ms']
            office_daily_average = office_hour.groupby(office_hour['Timestamp-UTC'].dt.date)[numeric_cols].mean()
            non_office_daily_average = non_office_hour.groupby(non_office_hour['Timestamp-UTC'].dt.date)[numeric_cols].mean()

            
            #add the date column
            office_daily_average['Timestamp-UTC'] = dateset
            non_office_daily_average['Timestamp-UTC'] = dateset

            #rename the columns
            office_daily_average.rename(columns={'LossRate-%':'Office Loss Rate Average',
                                    'AverageLatency-Ms':'Office Latency Average',
                                    'AverageJitter-Ms':'Office Jitter Average'}, 
                        inplace=True)
            
            non_office_daily_average.rename(columns={'LossRate-%':'Non-Office Loss Rate Average',
                                            'AverageLatency-Ms':'Non-Office Latency Average',
                                            'AverageJitter-Ms':'Non-Office Jitter Average'},
                                    inplace=True)
            

            daily_office_average = pd.concat([daily_office_average, office_daily_average], ignore_index=True)
            daily_non_office_average = pd.concat([daily_non_office_average, non_office_daily_average], ignore_index=True)


        return daily_office_average, daily_non_office_average

    def get_percentage(df):
        r'''
        get the percentage of 'LossRate-%', 'AverageLatency-Ms', 'AverageJitter-Ms' each day
        '''
        daily_office_percentage = pd.DataFrame()
        daily_non_office_percentage = pd.DataFrame()

        grouped = df.groupby(df['Timestamp-UTC'].dt.date)

        for dateset, group in grouped:
            office_hour, non_office_hour = data_DataProcess.get_daily_data(group)


            #percentage
            numeric_cols = ['LossRate-%', 'AverageLatency-Ms', 'AverageJitter-Ms']
            
            
            #get the Percentile 90
            office_daily_percentage = office_hour.groupby(office_hour['Timestamp-UTC'].dt.date)[numeric_cols].quantile(0.95)
            non_office_daily_percentage = non_office_hour.groupby(non_office_hour['Timestamp-UTC'].dt.date)[numeric_cols].quantile(0.95)
           

            #add the date column
            office_daily_percentage['Timestamp-UTC'] = dateset
            non_office_daily_percentage['Timestamp-UTC'] = dateset

            #rename the columns
            office_daily_percentage.rename(columns={'LossRate-%':'Office Loss Rate 95%',
                                    'AverageLatency-Ms':'Office Latency 95%',
                                    'AverageJitter-Ms':'Office Jitter 95%'},
                        inplace=True)
            
            non_office_daily_percentage.rename(columns={'LossRate-%':'Non-Office Loss Rate 95%',
                                            'AverageLatency-Ms':'Non-Office Latency 95%',
                                            'AverageJitter-Ms':'Non-Office Jitter 95%'},
                                    inplace=True)
            
            daily_office_percentage = pd.concat([daily_office_percentage, office_daily_percentage], ignore_index=True)
            daily_non_office_percentage = pd.concat([daily_non_office_percentage, non_office_daily_percentage], ignore_index=True)

        
        return daily_office_percentage, daily_non_office_percentage
            

if __name__ == '__main__':
    print('This is a module, please import it')
