import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
from pyspark.sql.functions import split
from pathlib import Path

# Set AWS credentials from your keys.cfg file
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create a Spark session with AWS configurations.
    """
    spark = SparkSession.builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3a://") \
        .enableHiveSupport().getOrCreate()
    return spark

def sas_to_date(date):
    """
    Convert SAS date to Python datetime.
    """
    return (datetime(1960, 1, 1) + timedelta(days=date)).strftime('%Y-%m-%d')

def read_labels(file, first_row, last_row):
    """
    Read labels from SAS descriptions file.
    """
    labels = {}
    with open(file) as f:
        file_content = f.readlines()
        for content in file_content[first_row:last_row]:
            content = content.split("=")
            code, cont = content[0].strip(), content[1].strip().strip("'")
            labels[code] = cont
    return labels

def process_immigration_data(spark, input_data, output_data):
    """
    Loads immigration data and processes it into fact and dimension tables.
    """
    immigration_data = os.path.join(input_data, "sas_data")
    df_immigration = spark.read.parquet(immigration_data)
    
    # Rename columns for clarity
    column_mapping = {
        'cic_id': 'cic_id',
        'i94yr': 'year',
        'i94mon': 'month',
        'i94cit': 'cit',
        'i94res': 'res',
        'i94port': 'port',
        'arrdate': 'arrival_date',
        'i94mode': 'mode',
        'i94addr': 'address',
        'depdate': 'departure_date',
        'i94age': 'age',
        'i94visa': 'visa',
        'count': 'count',
        'dtadfile': 'date_logged',
        'visapost': 'dept_visa_issuance',
        'occup': 'occupation',
        'entdepa': 'arrival_flag',
        'entdepd': 'departure_flag',
        'entdepu': 'update_flag',
        'matflag': 'match_flag',
        'biryear': 'birth_year',
        'dtaddto': 'max_stay_date',
        'gender': 'gender',
        'insnum': 'INS_number',
        'airline': 'airline',
        'admnum': 'admission_number',
        'fltno': 'flight_number',
        'visatype': 'visatype'
    }
    
    for old_col, new_col in column_mapping.items():
        df_immigration = df_immigration.withColumnRenamed(old_col, new_col)
    
    # Convert SAS dates to Python datetime
    sas_to_date_udf = udf(sas_to_date, DateType())
    df_immigration = df_immigration.withColumn("arrival_date", sas_to_date_udf("arrival_date"))
    df_immigration = df_immigration.withColumn("departure_date", sas_to_date_udf("departure_date"))
    
    # Read labels for countries and cities
    file = os.path.join(input_data, "I94_SAS_Labels_Descriptions.SAS")
    countries = read_labels(file, first_row=10, last_row=298)
    cities = read_labels(file, first_row=303, last_row=962)
    
    # Create dataframes for countries and cities
    countries_df = spark.createDataFrame(countries.items(), ['code', 'country'])
    cities_df = spark.createDataFrame(cities.items(), ['code', 'city'])
    
    # Extract state from city and split city into city and state
    cities_df = cities_df.withColumn('state', split(cities_df.city, ',').getItem(1)) \
                         .withColumn('city', split(cities_df.city, ',').getItem(0))
    
    # Create unique identifiers for cities and countries
    cities_df = cities_df.select([monotonically_increasing_id().alias('id'), '*'])
    countries_df = countries_df.select([monotonically_increasing_id().alias('id'), '*'])
    
    # Create fact and dimension tables
    immigration_fact = df_immigration.select(['cic_id', 'year', 'month', 'arrival_date', 'departure_date', 'mode', 'visatype']).distinct()
    dim_immigrants = df_immigration.select([monotonically_increasing_id().alias('id'), 'cic_id', 'cit', 'res', 'visa', 'age', 'occupation', 'gender', 'address', 'INS_number']).distinct()
    dim_flight_details = df_immigration.select([monotonically_increasing_id().alias('id'), 'cic_id', 'flight_number', 'airline']).distinct()
    
    # Write dataframes to parquet files
    countries_df.write.mode('overwrite').parquet(output_data + "countries.parquet")
    cities_df.write.mode('overwrite').parquet(output_data + "cities.parquet")
    immigration_fact.write.mode('overwrite').parquet(output_data + "fact_immigration.parquet")
    dim_immigrants.write.mode('overwrite').parquet(output_data + "dim_immigrants.parquet")
    dim_flight_details.write.mode('overwrite').parquet(output_data + "dim_flight_details.parquet")

def process_demographics_data(spark, input_data, output_data):
    """
    Loads city demographics data and processes it into dimension tables.
    """
    city_data = os.path.join(input_data, "us-cities-demographics.csv")
    city_df = spark.read.option("delimiter", ";").option("header", "true").csv(city_data)
    
    city_demography = city_df.select(['City', 'State', 'State Code', 'Male Population', 'Female Population', 'Foreign-born', 'Number of Veterans', 'Race']).distinct()
    city_stats = city_df.select(['City', 'State Code', 'Median Age', 'Average Household Size', 'Count']).distinct()
    
    city_demography.write.mode('overwrite').parquet(output_data + "city_demography.parquet")
    city_stats.write.mode('overwrite').parquet(output_data + "city_stats.parquet")

def process_airport_data(spark, input_data, output_data):
    """
    Loads airport data and processes it into dimension tables.
    """
    airport_data = os.path.join(input_data, "airport-codes_csv.csv")
    airport_df = spark.read.option("header", "true").csv(airport_data)
    
    airport_df = airport_df.filter((airport_df.iso_country == "US") & (airport_df.type != "closed")).distinct()
    
    airport_dim = airport_df.select(["ident", "type", "name", "continent", "gps_code", "iata_code", "iso_country"]).distinct()
    airport_stats = airport_df.select(["ident", "elevation_ft", "coordinates"]).distinct()
    
    airport_dim.write.mode('overwrite').parquet(output_data + "airports_dim.parquet")
    airport_stats.write.mode('overwrite').parquet(output_data + "airports_stats.parquet")

def main():
    input_data = "s3a://udacity-dend/"
    output_data = ""  # Specify your desired S3 output path
    spark = create_spark_session()
    process_immigration_data(spark, input_data, output_data)
    process_demographics_data(spark, input_data, output_data)
    process_airport_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
