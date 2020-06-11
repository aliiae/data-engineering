import configparser
import csv
from typing import Dict

from pyspark import Broadcast
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

from util import (
    _add_country_name,
    _add_location,
    _add_region_name,
    _get_ppd_headers,
    _normalise_address,
    _normalise_postcode,
    logger,
)

config = configparser.ConfigParser()
config.read("credentials.cfg")

PPD_PATH = config.get("S3", "SPARK_INPUT_PPD")
PPD_HEADERS = config.get("S3", "SPARK_INPUT_PPD_HEADERS")
POSTCODE_PATH = config.get("S3", "SPARK_INPUT_POSTCODES")
REGION_NAMES = config.get("S3", "SPARK_INPUT_REGION_NAMES")
OUTPUT_DATA = config.get("S3", "OUTPUT")


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    logger.info("Created Spark session")
    return spark


def write_property_table(
    df: DataFrame, output_path, filename: str = "property.parquet"
) -> DataFrame:
    """
    Extract a property dimension table from the staging data and save it in the parquet format.
    Args:
        df: Staging dataframe containing source data.
        output_path: Path to where the resulting parquet files are saved.
        filename: Optional filename of the resulting parquet directory in the output path.

    Returns: A property dimension dataframe.
    """
    property_table = (
        df.where(f.col("property_type").isNotNull())
        .drop_duplicates(["property_type", "is_new", "duration"])
        .withColumn("property_id", f.monotonically_increasing_id())
        .select(["property_id", "property_type", "is_new", "duration"])
    )
    property_table.write.parquet(f"{output_path}/{filename}")
    logger.info("Saved property table")
    return property_table


def write_time_table(
    df: DataFrame, output_path, filename: str = "time.parquet"
) -> DataFrame:
    """
    Extract a time dimension table from the staging data and save it in the parquet format.
    Args:
        df: Staging dataframe containing source data.
        output_path: Path to where the resulting parquet files are saved.
        filename: Optional filename of the resulting parquet directory in the output path.

    Returns: A time dimension dataframe.
    """
    time_table = df.drop_duplicates(["date"]).where(f.col("date").isNotNull())
    time_table = (
        time_table.select(["date"])
        .withColumn("year", f.year(time_table["date"]))
        .withColumn("month", f.month(time_table["date"]))
        .withColumn("day", f.dayofmonth(time_table["date"]))
        .withColumn("week", f.weekofyear(time_table["date"]))
        .withColumn("weekday", f.dayofweek(time_table["date"]))
    )
    time_table.write.parquet(f"{output_path}/{filename}")
    logger.info("Saved time table")
    return time_table


def write_sale_table(
    df: DataFrame,
    property_table: DataFrame,
    output_path: str,
    filename: str = "sale.parquet",
) -> DataFrame:
    """
    Extract a sale transaction (fact) table from the staging data and save it in the parquet format.
    Args:
        df: Staging dataframe containing source data.
        property_table: Property dimension dataframe to join and extract property id.
        output_path: Path to where the resulting parquet files are saved.
        filename: Optional filename of the resulting parquet directory in the output path.

    Returns: A sale transaction dataframe.
    """
    sale_table = (
        df.where(f.col("transaction_unique_identifier").isNotNull())
        .withColumn(
            "id",
            f.udf(lambda x: x.strip("}").strip("{"), t.StringType())(
                f.col("transaction_unique_identifier")
            ),
        )
        .withColumn("price", df["price"].cast(t.IntegerType()))
        .withColumn("year", f.year(df["date"]))
        .withColumn("month", f.month(df["date"]))
    )
    sale_table = _normalise_address(sale_table)
    sale_table = _normalise_postcode(sale_table)
    sale_table = sale_table.join(
        property_table,
        [
            property_table.property_type == sale_table.property_type,
            property_table.is_new == sale_table.is_new,
            property_table.duration == sale_table.duration,
        ],
    ).select(
        ["id", "price", "date", "property_id", "postcode", "address", "year", "month"]
    )
    sale_table.write.partitionBy(["year", "month"]).parquet(f"{output_path}/{filename}")
    logger.info("Saved sale table")
    return sale_table


def get_ppd_tables(
    spark: SparkSession, input_path, output_path: str, headers_path: str
) -> Dict[str, DataFrame]:
    """
    Process Price Paid Dataset (PPD) by extracting fact and dimension tables
    and saving them to S3 in the parquet format.
    Args:
        spark: Current Spark session object.
        input_path: Path to the PPD dataset in the CSV format.
        output_path: Path where the resulting parquet files are saved.
        headers_path: Path to the PPD headers TSV file.

    Returns: A dictionary mapping table name to its dataframe.
    The full staging dataset is mapped as "original".
    """
    headers = _get_ppd_headers(headers_path)
    df = spark.read.csv(input_path, header=False).toDF(*headers)
    df = df.filter("record_status = 'A'")
    property_types = {
        "D": "detached",
        "S": "semi-detached",
        "T": "terraced",
        "F": "flat",
        "O": "other",
    }
    property_types = spark.sparkContext.broadcast(property_types)

    df = (
        df.withColumn(
            "property_type",
            f.udf(lambda x: property_types.value[x], t.StringType())(
                f.col("property_type")
            ),
        )
        .withColumn(
            "is_new",
            f.udf(lambda x: True if x == "Y" else False, t.BooleanType())(
                f.col("old_new")
            ),
        )
        .withColumn(
            "duration",
            f.udf(lambda x: "freehold" if x == "F" else "leasehold", t.StringType())(
                f.col("duration")
            ),
        )
        .withColumn("date", f.to_date(df["date_of_transfer"]))
        .select(
            [column for column in df.columns if column not in {"old_new"}]
            + ["is_new", "date"]
        )
    )
    tables = {
        "original": df,
        "property": write_property_table(df, output_path),
        "time": write_time_table(df, output_path),
    }
    return tables


def get_postcode_tables(
    spark: SparkSession, input_path: str, output_path: str, region_names_path: str
) -> Dict[str, DataFrame]:
    """
    Process Code-Point Open dataset by extracting a postcode dimension table
    and saving it to S3 in the parquet format.

    Args:
        spark: Current Spark session.
        input_path: Path to the postcodes dataset file.
        output_path: Path to where the resulting parquet files are saved.
        region_names_path: Path to the CSV mapping of the UK administrative codes to postcodes.

    Returns: A dictionary mapping table names to their dataframes.
    """
    headers = [
        "postcode",
        "positional_quality_indicator",
        "eastings",
        "northings",
        "country_code",
        "nhs_regional_ha_code",
        "nhs_ha_code",
        "admin_county_code",
        "admin_district_code",
        "admin_ward_code",
    ]
    df = spark.read.csv(input_path, header=False).toDF(*headers)
    countries = {"E": "England", "W": "Wales", "S": "Scotland", "N": "Northern Ireland"}
    with open(region_names_path) as region_names_file:
        reader = csv.reader(region_names_file)
        region_names = {row[1]: row[0] for row in reader}
    region_names = spark.sparkContext.broadcast(region_names)
    countries = spark.sparkContext.broadcast(countries)
    tables = {
        "postcode": write_postcode_table(df, region_names, countries, output_path)
    }
    return tables


def write_postcode_table(
    df: DataFrame,
    region_names: Broadcast,
    countries: Broadcast,
    output_path: str,
    filename: str = "postcode.parquet",
) -> DataFrame:
    """
    Extract a postcode dimension table from the staging data and save it in the parquet format.
    Args:
        df: Staging dataframe containing source data.
        region_names: Mapping of region codes to their names.
        countries: Mapping of the initial letters of the country codes to the country name.
        output_path: Path to where the resulting parquet files are saved.
        filename: Optional filename of the resulting parquet directory in the output path.

    Returns: A postcode dimension dataframe.
    """
    postcode_table = _add_location(df)
    postcode_table = _normalise_postcode(postcode_table)
    postcode_table = _add_country_name(postcode_table, countries)
    postcode_table = _add_region_name(postcode_table, region_names)
    postcode_table = postcode_table.select(
        ["postcode", "district", "country", "longitude", "latitude"]
    )
    postcode_table.write.partitionBy(["district"]).parquet(f"{output_path}/{filename}")
    logger.info("Saved postcode table")
    return postcode_table


def main():
    """Read data from S3, process the data using Spark, and write it back to S3."""
    spark = create_spark_session()

    ppd_tables = get_ppd_tables(spark, PPD_PATH, OUTPUT_DATA, PPD_HEADERS)
    get_postcode_tables(spark, POSTCODE_PATH, OUTPUT_DATA, REGION_NAMES)
    write_sale_table(ppd_tables["original"], ppd_tables["property"], OUTPUT_DATA)

    spark.stop()


if __name__ == "__main__":
    main()
