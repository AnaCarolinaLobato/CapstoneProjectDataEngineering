from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
import configparser

# Read AWS credentials from config file
config = configparser.ConfigParser()
config.read('aws.cfg')

# Set AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# Create a Spark session
def create_spark_session():
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    return spark

# Define the expected schema for tables
expected_schemas = {
    "fact_immigration": [
        ("cic_id", "double"),
        ("year", "double"),
        ("month", "double"),
        ("arrival_date", "date"),
        ("departure_date", "date"),
        ("mode", "double"),
        ("visatype", "string"),
    ],
    "airports_dim": [
        ("ident", "string"),
        ("type", "string"),
        ("name", "string"),
        ("continent", "string"),
        ("gps_code", "string"),
        ("iata_code", "string"),
        ("local_code", "string"),
        ("iso_country", "string"),
    ],
    "airports_stats": [
        ("ident", "string"),
        ("elevation_ft", "string"),
        ("coordinates", "string"),
    ],
    "city_stats": [
        ("City", "string"),
        ("State Code", "string"),
        ("Median Age", "string"),
        ("Average Household Size", "string"),
        ("Count", "string"),
    ],
    "city_demography": [
        ("City", "string"),
        ("State", "string"),
        ("State Code", "string"),
        ("Male Population", "string"),
        ("Female Population", "string"),
        ("Foreign-born", "string"),
        ("Number of Veterans", "string"),
        ("Race", "string"),
    ],
    "countries": [
        ("code", "string"),
        ("country", "string"),
    ],
    "cities": [
        ("code", "string"),
        ("city", "string"),
        ("state", "string"),
    ]
}

def check_table_schemas(spark, output_path):
    for table, schema in expected_schemas.items():
        table_path = Path(output_path) / f"{table}.parquet"
        if table_path.is_dir():
            df = spark.read.parquet(str(table_path))
            for field, data_type in schema:
                if field not in df.columns or df.schema[field].dataType.simpleString() != data_type:
                    raise ValueError(f"Schema mismatch in table {table}. Field {field} has incorrect data type.")

def check_empty_tables(spark, output_path):
    for table, schema in expected_schemas.items():
        table_path = Path(output_path) / f"{table}.parquet"
        if table_path.is_dir():
            df = spark.read.parquet(str(table_path))
            if df.count() == 0:
                print(f"Empty table: {table}")
            else:
                print(f"Table {table} is not empty")

if __name__ == '__main__':
    spark = create_spark_session()
    output_path = ""
    check_table_schemas(spark, output_path)
    check_empty_tables(spark, output_path)

