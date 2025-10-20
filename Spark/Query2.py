import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions

spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
print("Session Started.")
try:
        data_agn = spark.read.option('header', True).csv('Data/agn.us.txt')
        data_ainv = spark.read.option('header', True).csv('Data/ainv.us.txt')
        data_ale = spark.read.option('header', True).csv('Data/ale.us.txt')
        # Cast data to proper types
        data_agn = data_agn.withColumn('Open', functions.col('Open').cast('double')) \
        .withColumn('High', functions.col('High').cast('double')) \
        .withColumn('Low', functions.col('Low').cast('double')) \
        .withColumn('Close', functions.col('Close').cast('double')) \
        .withColumn('Volume', functions.col('Volume').cast('integer')) \
        .withColumn('OpenInt', functions.col('OpenInt').cast('integer'))
        data_ainv = data_ainv.withColumn('Open', functions.col('Open').cast('double')) \
        .withColumn('High', functions.col('High').cast('double')) \
        .withColumn('Low', functions.col('Low').cast('double')) \
        .withColumn('Close', functions.col('Close').cast('double')) \
        .withColumn('Volume', functions.col('Volume').cast('integer')) \
        .withColumn('OpenInt', functions.col('OpenInt').cast('integer'))
        data_ale = data_ale.withColumn('Open', functions.col('Open').cast('double')) \
        .withColumn('High', functions.col('High').cast('double')) \
        .withColumn('Low', functions.col('Low').cast('double')) \
        .withColumn('Close', functions.col('Close').cast('double')) \
        .withColumn('Volume', functions.col('Volume').cast('integer')) \
        .withColumn('OpenInt', functions.col('OpenInt').cast('integer'))
        print('Loaded share value data for AGN, AINV and ALE on Spark.')

        # 2.1.X Which was the average of all opening prices, closing prices and trading volume for each share per calendar month?  
        print('Task 2.1.1: The average Opening Price, Closing Price and Trade Volume of AGN for each calendar month was:')
        res = data_agn.groupBy(data_agn.Date.substr(0,7)).agg(functions.avg('Open'), functions.avg('Close'), functions.avg('Volume')).sort(functions.asc('substring(Date, 0, 7)')).collect()
        for row in res:
                print('Month: {month} | Opening Price: {open:.2f} USD | Closing Price: {close:.2f} USD | Volume: {vol:,}'.format(month=row['substring(Date, 0, 7)'], open=row['avg(Open)'], close=row['avg(Close)'], vol=int(row['avg(Volume)'])))

        print('Task 2.1.2: The average Opening Price, Closing Price and Trade Volume of AINV for each calendar month was:')
        res = data_ainv.groupBy(data_ainv.Date.substr(0,7)).agg(functions.avg('Open'), functions.avg('Close'), functions.avg('Volume')).sort(functions.asc('substring(Date, 0, 7)')).collect()
        for row in res:
                print('Month: {month} | Opening Price: {open:.2f} USD | Closing Price: {close:.2f} USD | Volume: {vol:,}'.format(month=row['substring(Date, 0, 7)'], open=row['avg(Open)'], close=row['avg(Close)'], vol=int(row['avg(Volume)'])))

        print('Task 2.1.3: The average Opening Price, Closing Price and Trade Volume of ALE for each calendar month was:')
        res = data_ale.groupBy(data_ale.Date.substr(0,7)).agg(functions.avg('Open'), functions.avg('Close'), functions.avg('Volume')).sort(functions.asc('substring(Date, 0, 7)')).collect()
        for row in res:
                print('Month: {month} | Opening Price: {open:.2f} USD | Closing Price: {close:.2f} USD | Volume: {vol:,}'.format(month=row['substring(Date, 0, 7)'], open=row['avg(Open)'], close=row['avg(Close)'], vol=int(row['avg(Volume)'])))
        
        # 2.2.X For how many days was the opening price of each share above 35 USD?
        res = data_agn.filter(data_agn.Open > 35).count()
        print('Task 2.2.1: The Opening Price of AGN was higher than 35 USD for {days} days.'.format(days=res))

        res = data_ainv.filter(data_ainv.Open > 35).count()
        print('Task 2.2.2: The Opening Price of AINV was higher than 35 USD for {days} days.'.format(days=res))

        res = data_ale.filter(data_ale.Open > 35).count()
        print('Task 2.2.3: The Opening Price of ALE was higher than 35 USD for {days} days.'.format(days=res))

        # 2.3.Xa Which were the days with the highest opening price for each share?
        res = data_agn.agg(functions.max_by('Date', 'Open').alias('Max_day')).collect()
        print('Task 2.3.1a: The day on which AGN hit its highest Opening Price was {date}.'.format(date=res[0]['Max_day']))

        res = data_ainv.agg(functions.max_by('Date', 'Open').alias('Max_day')).collect()
        print('Task 2.3.2a: The day on which AINV hit its highest Opening Price was {date}.'.format(date=res[0]['Max_day']))

        res = data_ale.agg(functions.max_by('Date', 'Open').alias('Max_day')).collect()
        print('Task 2.3.3a: The day on which ALE hit its highest Opening Price was {date}.'.format(date=res[0]['Max_day']))

        # 2.3.Xb Which were the days with the highest transaction volume for each share?
        res = data_agn.agg(functions.max_by('Date', 'Volume').alias('Max_day')).collect()
        print('Task 2.3.1b: The day on which AGN hit its highest Transaction Volume was {date}.'.format(date=res[0]['Max_day']))

        res = data_ainv.agg(functions.max_by('Date', 'Volume').alias('Max_day')).collect()
        print('Task 2.3.2b: The day on which AINV hit its highest Transaction Volume was {date}.'.format(date=res[0]['Max_day']))

        res = data_ale.agg(functions.max_by('Date', 'Volume').alias('Max_day')).collect()
        print('Task 2.3.3b: The day on which ALE hit its highest Transaction Volume was {date}.'.format(date=res[0]['Max_day']))

        # 2.4.Xa Which were the years that each share reached the highest opening price?
        res = data_agn.agg(functions.max_by(data_agn.Date.substr(0,4), 'Open').alias('Max_year')).collect()
        print('Task 2.4.1a: The year during which AGN hit its highest Opening Price was {year}.'.format(year=res[0]['Max_year']))

        res = data_ainv.agg(functions.max_by(data_ainv.Date.substr(0,4), 'Open').alias('Max_year')).collect()
        print('Task 2.4.2a: The year during which AINV hit its highest Opening Price was {year}.'.format(year=res[0]['Max_year']))

        res = data_ale.agg(functions.max_by(data_ale.Date.substr(0,4), 'Open').alias('Max_year')).collect()
        print('Task 2.4.3a: The year during which ALE hit its highest Opening Price was {year}.'.format(year=res[0]['Max_year']))

        # 2.4.Xb Which were the years that each share reached the lowest closing price?
        res = data_agn.agg(functions.min_by(data_agn.Date.substr(0,4), 'Close').alias('Min_year')).collect()
        print('Task 2.4.1b: The year during which AGN hit its lowest Closing Price was {year}.'.format(year=res[0]['Min_year']))

        res = data_ainv.agg(functions.min_by(data_ainv.Date.substr(0,4), 'Close').alias('Min_year')).collect()
        print('Task 2.4.2b: The year during which AINV hit its lowest Closing Price was {year}.'.format(year=res[0]['Min_year']))

        res = data_ale.agg(functions.min_by(data_ale.Date.substr(0,4), 'Close').alias('Min_year')).collect()
        print('Task 2.4.3b: The year during which ALE hit its lowest Closing Price was {year}.'.format(year=res[0]['Min_year']))

        spark.stop()
        print('Session Stopped.')
except Exception as err:
        spark.stop()
        print('Door Stuck.')
        print(err)