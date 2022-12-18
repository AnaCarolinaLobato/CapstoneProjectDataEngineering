import pandas as pd
import os
import configparser

from datetime import datetime, timedelta
from email import header
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col, array_contains, split, monotonically_increasing_id,year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import DateType, StringType

config = configparser.ConfigParser()
config.read('keys.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']

def createsparksession():
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()

    return spark


def sas_to_date(date):
    # 1. sas dates to python datetime
    """
    datetime.strptime('2016-01-01', '%Y-%m-%d') - timedelta(20566) gives a date between the year 1959 and 1960 
    working with the date 1960-01-01 gives a more accurate date that reflects the arrival and departure dates
    """
    return (datetime.strptime('1960-01-01', '%Y-%m-%d') + timedelta(date)).strftime('%Y-%m-%d')


def read_labels(file, first_row,last_row):
    frame = {}
    frame2 = {}
    print(first_row)
    with open(file) as f:
        file_content = f.readlines()
        
        for content in file_content[first_row:last_row]:
            content = content.split("=")
            if first_row ==  303:
                code, cont= content[0].strip("\t").strip().strip("'"),content[1].strip("\t").strip().strip("''")
            else:
                code, cont = content[0].strip(),content[1].strip().strip("'")
            frame[code] =cont
    return frame


def process_immigration_data(spark, input_data, output_data):
    """
    Loads immigration data and processes it into a fact table
    input_data: source of data
    output_data: destination storage for processed data
    """
    # read/load immigration_data file
    immigration_data = os.path.join( input_data + "sas_data" )

    # load data
    df_immigration = spark.read.parquet(immigration_data)
    
    # extract columns to create table
    df_immigration = df_immigration.select("*")
        
    # rename columns to make them inteligible
    rename_cols=["cic_id","year","month","cit","res","port","arrival_date","mode","address","departure_date","age","visa","count","date_logged",
                 "dept_visa_issuance","occupation","arrival_flag","departure_flag","update_flag","match_flag","birth_year","max_stay_date","gender",
                 "INS_number","airline","admission_number","flight_number","visatype"]
    
    #create an iterator between collumns
    for col, rename_cols in zip(df_immigration.columns, rename_cols):
        df_immigration = df_immigration.withColumnRenamed(col, rename_cols)
    
    immigration_fact = df_immigration[
        [
            "cic_id",
            "year",
            "month",
            "arrival_date",
            "departure_date",
            "mode",
            "visatype",
        ]
    ].dropDuplicates(['cic_id'])
    
    # dimensions from immigration data
    dim_flight_details = df_immigration.select([monotonically_increasing_id().alias('id'),'cic_id','flight_number','airline']).dropDuplicates()
    dim_immigrants= df_immigration.select([monotonically_increasing_id().alias('id'),'cic_id','cit','res','visa',
                                           'age','occupation','gender','address','INS_number']).dropDuplicates()
    
    # organazing data
    udf_func = udf(sas_to_date,DateType())
    immigration_fact = immigration_fact.withColumn("arrival_date",udf_func("arrival_date"))
    immigration_fact = immigration_fact.withColumn("departure_date",udf_func("departure_date"))
    file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    countries = read_labels(file, first_row=10, last_row=298)
    cities = read_labels(file,first_row=303,last_row=962)
    countries_df = spark.createDataFrame(countries.items(), ['code', 'country']).dropDuplicates()
    cities_df = spark.createDataFrame(cities.items(), ['code', 'city']).dropDuplicates()
    cities_df = cities_df.withColumn('state', split(cities_df.city,',').getItem(1))\
            .withColumn('city', split(cities_df.city,',').getItem(0))
    cities_df = cities_df.select([monotonically_increasing_id().alias('id'),'*'])
    countries_df = countries_df.select([monotonically_increasing_id().alias('id'),'*'])
    
    # write parquet
    countries_df.write.mode('overwrite').parquet( "countries_df.parquet")
    cities_df.write.mode('overwrite').parquet("cities_df.parquet")
    immigration_fact.write.mode('overwrite').parquet("immigration_fact.parquet")
    dim_immigrants.write.parquet("dim_immigrants.parquet")
    dim_flight_details.write.parquet("dim_flight_details.parquet")
    
    
def process_demographics_data(spark, input_data, output_data):
    """
    loads and processes city data
    input_data: path to data source
    output_data: path to data store
    """

    # fetch data path
    city_data = os.path.join(input_data + "us-cities-demographics.csv")

    # load city data
    city_df = spark.read.load(city_data, sep=";", format="csv", header=True)

    # Demography, examine the city based on size, and structure (Gender & Ethnicity composition)
    city_demography = city_df.select(
        [
            "City",
            "State",
            "State Code",
            "Male Population",
            "Female Population",
            "Foreign-born",
            "Number of Veterans",
            "Race",
        ]
    ).dropDuplicates()
    city_demography.write.parquet(output_data + "city_demography.parquet")

    # statistics on city such as total population, and median age
    city_stats = city_df.select(
        ["City", "State Code", "Median Age", "Average Household Size", "Count"]
    ).dropDuplicates()
    city_stats.write.mode('overwrite').parquet(output_data + "city_stats.parquet")
    
    
def process_airport_data(spark, input_data, output_data):
    """
    loads and processes city data
    input_data: path to data source
    output_data: path to data store
    """

    # fetch data source
    airport_data = os.path.join(input_data + "airport-codes_csv.csv")
    
    # load data
    airport_df = spark.read.csv(airport_data, header=True)
    airport_df = airport_df.filter(
        (airport_df.iso_country == "US") & ~(airport_df.type == "closed")
    ).dropDuplicates()
    
    # data quality check (unique identity column)
    check_unique_id_column(airport_df,'idents')
    
    airport_dim = airport_df.select(["ident","type","name","continent","gps_code","iata_code","local_code","iso_country"]).dropDuplicates()
    airport_stats = airport_df.select(["ident", "elevation_ft", "coordinates"]).dropDuplicates()

    airport_dim.write.parquet(output_data + "airports_dim.parquet")
    airport_stats.write.parquet(output_data + "airports_stats.parquet")
    
    
def main():
    input_data = "s3a://udacity-dend/"
    output_data = ""
    spark = createsparksession()
    process_immigration_data(spark, output_data, input_data)
    process_demographics_data(spark, output_data, input_data)
    process_airport_data(spark, output_data, input_data)
    


if __name__ == "__main__":
    main()
