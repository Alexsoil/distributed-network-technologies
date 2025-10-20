import findspark
findspark.init()
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Row

spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
print("Session Started.")
try:
        tour_df = pd.read_excel('Data/tour_occ_ninat.xlsx', sheet_name=0, header=8)
        tour_df.replace(':', None, inplace=True)
        tour_df.map(lambda x: x if pd.isnull(x) or isinstance(x, str) else int(x))
        data_tour = spark.createDataFrame(tour_df)
        print('Loaded EU tourism data on Spark.')
        
        # Task 3
        # 3.1.1 Which was the average number of accomodations for each country in the range 2007-2014?
        mean_years = ('2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014')
        def acc_mean(row: Row, years: tuple):
                year_list = []
                country = row['GEO/TIME']
                for year in years:
                        year_list.append(row[year])
                while None in year_list:
                        year_list.remove(None)
                if len(year_list) == 0:
                        print('{}: No data for the time period.'.format(country))
                elif len(years) > len(year_list):
                        print('{country}: {acc} (No data for {missing} out of {total} years.)'.format(country=country, acc=int(sum(year_list)/len(year_list)), missing=(len(years) - len(year_list)), total=len(years)))
                else:
                        print('{country}: {acc}'.format(country=country, acc=int(sum(year_list)/len(year_list))))
        print('Task 3.1.1: The average number of accomodations in the time period 2007-2014 for each EU country was:')
        data_tour.foreach(lambda x: acc_mean(x, mean_years))

        # 3.2.1 For how many, and which years was the number of accomodations of Greece higher than 5 other EU countries (my choice)?
        countries = ['Greece', 'Germany (until 1990 former territory of the FRG)', 'Bulgaria', 'Belgium', 'Croatia', 'Netherlands']
        years = ('2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015')
        best_list = []
        for year in years:
                res = data_tour.filter(functions.col('GEO/TIME').isin(countries)).agg(functions.max_by('GEO/TIME', year).alias('Country')).collect()
                if res[0]['Country'] == 'Greece':
                        best_list.append(year)
        print('Task 3.2.1: Greece had more accomodations than the following countries for {number} years which were: '.format(number=len(best_list)), end="")
        print(*best_list, sep=', ')
        print('Selected countries:')
        print(*countries, sep=', ')

        # 3.3.1 Which where the countries with the highest number of accomodations for each year?
        years = ('2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015')
        print('Task 3.3.1: The three EU countries with the highest accomodations for each year were:')
        for year in years:
                res = data_tour.sort(year, ascending=False).select('GEO/TIME').head(3)
                print('{year}: 1) {one} 2) {two} 3) {three}'.format(year=year, one=res[0]['GEO/TIME'], two=res[1]['GEO/TIME'], three=res[2]['GEO/TIME']))
        
        # 3.4.1 Which was the year each country had the lowest number of accomodations in comparisson to every other EU country?
        years = ('2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015')
        res = data_tour.select('GEO/TIME', functions.least('2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015').alias('Least')).collect()
        # This is stupid :^)
        output = []
        for row in res:
                for year in years:
                        year_name = data_tour.select('GEO/TIME', year).filter(functions.col('GEO/TIME') == row['GEO/TIME']).head()
                        if row['Least'] == year_name[year]:
                                output.append((row['GEO/TIME'], year))
                                break
        print('Task 3.4.1: The year during which each EU country had the least accomodations were the following:')
        for item in output:
                print('{country}: {year}'.format(country=item[0], year=item[1]))

        spark.stop()
        print('Session Stopped.')
except Exception as err:
        spark.stop()
        print('Door Stuck.')
        print(err)