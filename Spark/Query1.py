import findspark
findspark.init()
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions

def load_json(source: str) -> pd.DataFrame:
        filetest = open(source, 'r')
        lines = filetest.readlines()
        big_df = pd.DataFrame(columns=['Timestamp', 'Value'])
        for line in lines:
                temp = json.loads(line)
                df = pd.DataFrame.from_dict(temp, orient="index")
                df.reset_index(inplace=True)
                df.rename(columns={'index': 'Timestamp', 0: 'Value'}, inplace=True)
                df.sort_values(by=['Timestamp'], inplace=True)
                big_df = pd.concat([big_df, df])
        big_df.reset_index(inplace=True)
        big_df.drop(columns='index', inplace=True)
        return big_df

spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
print("Session Started.")
try:
        temp_df = load_json('Data/tempm.txt')
        hum_df = load_json('Data/hum.txt')
        combine_df = temp_df.merge(hum_df, on='Timestamp', how='outer', suffixes=('_temp', '_hum'))
        combine_df[['Date', 'Time']] = combine_df['Timestamp'].str.split('T', n=1, expand=True)
        combine_df[['Value_temp', 'Value_hum']] = combine_df[['Value_temp', 'Value_hum']].apply(pd.to_numeric)
        combine_df.dropna(inplace=True)
        data_temphum = spark.createDataFrame(combine_df)
        data_temphum = data_temphum.withColumn('Value_temp', functions.col('Value_temp').cast('double')).withColumn('Value_hum', functions.col('Value_hum').cast('double'))
        print('Loaded temperature and humidity data on Spark.')

        # 1.1.1 On how many days did the temperature range from 18C to 22C?
        days_to_delete = data_temphum.filter((data_temphum.Value_temp < 18) | (data_temphum.Value_temp > 22)).drop('Timestamp', 'Time', 'Value_temp', 'Value_hum').dropDuplicates()
        res = data_temphum.select('Date').dropDuplicates().exceptAll(days_to_delete).count()
        print('Task 1.1.1: The number of days on which the temperature ranged from 18 to 22 degrees Celsius was {days} days.'.format(days = res))

        # 1.2.1 Which where the 10 coldest days?
        res = data_temphum.groupBy('Date').agg(functions.avg('Value_temp').alias('Average_temp')).sort(functions.asc('Average_temp')).head(10)
        print('Task 1.2.1: The 10 days with the lowest average temperature were the following:')
        for row in res:
                print('{date} ({temp:.3f} C)'.format(date=row['Date'], temp=row['Average_temp']))

        # 1.2.2 Which where the 10 hottest days?
        res = data_temphum.groupBy('Date').agg(functions.avg('Value_temp').alias('Average_temp')).sort(functions.desc('Average_temp')).head(10)
        print('Task 1.2.2: The 10 days with the highest average temperature were the following:')
        for row in res:
                print('{date} ({temp:.3f} C)'.format(date=row['Date'], temp=row['Average_temp']))

        # 1.3.1 Which month had the highest standard deviation in humidity values?
        res = data_temphum.groupBy(data_temphum.Date.substr(0,7)).agg(functions.stddev('Value_hum').alias('Humidity_stddev')).sort(functions.desc('Humidity_stddev')).head()
        print('Task 1.3.1: The month with the highest standard deviation in humidity values was {month} ({stddev:.3}).'.format(month=res[0], stddev=res[1]))

        # 1.4.1 Which was the maximum value of the dysphoria index?
        dysphoria = data_temphum.withColumn('DI', (data_temphum.Value_temp - 0.55 * (1 - 0.01 * data_temphum.Value_hum) * (data_temphum.Value_temp - 14.5)))
        res = dysphoria.sort(functions.desc('DI')).head()
        print('Task 1.4.1: The maximum value of the dysphoria index was {dys:.3}.'.format(dys=res['DI']))

        # 1.4.2 Which was the minimum value of the dysphoria index?
        res = dysphoria.sort(functions.asc('DI')).head()
        print('Task 1.4.2: The minimum value of the dysphoria index was {dys:.3}.'.format(dys=res['DI']))
        
        spark.stop()
        print('Session Stopped.')
except Exception as err:
        spark.stop()
        print('Door Stuck.')
        print(err)