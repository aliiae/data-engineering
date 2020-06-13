import configparser
import csv
import logging
from typing import Dict, List, Optional

import pandas as pd
import utm
from pyspark import Broadcast
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    df: DataFrame, output_path, filename: str = "property.csv"
) -> DataFrame:
    """
    Extract a property dimension table from the staging data and save it in the csv format.
    Args:
        df: Staging dataframe containing source data.
        output_path: Path to where the resulting csv files are saved.
        filename: Optional filename of the resulting csv directory in the output path.


    Returns: A property dimension dataframe.
    """
    property_table = (
        df.where(f.col("property_type").isNotNull())
        .drop_duplicates(["property_address"])
        .select(["property_address", "property_type", "is_new", "duration"])
    )
    if output_path:
        property_table.write.csv(f"{output_path}/{filename}")
        logger.info("Saved property table")
    return property_table


def write_time_table(
    df: DataFrame, output_path, filename: str = "time.csv",
) -> DataFrame:
    """
    Extract a time dimension table from the staging data and save it in the csv format.
    Args:
        df: Staging dataframe containing source data.
        output_path: Path to where the resulting csv files are saved.
        filename: Optional filename of the resulting csv directory in the output path.


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
    if output_path:
        time_table.write.csv(f"{output_path}/{filename}")
        logger.info("Saved time table")
    return time_table


def write_sale_table(
    df: DataFrame, output_path: str, filename: str = "sale.csv",
) -> DataFrame:
    """
    Extract a sale transaction (fact) table from the staging data and save it in the csv format.
    Args:
        df: Staging dataframe containing source data.
        output_path: Path to where the resulting csv files are saved.
        filename: Optional filename of the resulting csv directory in the output path.


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
    sale_table = _normalise_postcode(sale_table)
    sale_table = sale_table.select(
        ["id", "price", "date", "postcode", "property_address", "year", "month"]
    )
    if output_path:
        sale_table.write.partitionBy(["year", "month"]).csv(f"{output_path}/{filename}")
        logger.info("Saved sale table")
    return sale_table


def get_ppd_tables(
    spark: SparkSession,
    input_path,
    headers_path: str,
    output_path: Optional[str] = None,
) -> Dict[str, DataFrame]:
    """
    Process Price Paid Dataset (PPD) by extracting fact and dimension tables
    and saving them to S3 in the csv format.
    Args:
        spark: Current Spark session object.
        input_path: Path to the PPD dataset in the CSV format.
        output_path: Path where the resulting csv files are saved.
        headers_path: Path to the PPD headers TSV file.

    Returns: A dictionary mapping table name to its dataframe.
    """
    df = read_ppd_table(spark, input_path, headers_path)
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
    )
    df = _normalise_address(df).select(
        [column for column in df.columns if column not in {"old_new"}]
        + ["is_new", "date", "property_address"]
    )
    tables = {
        "property": write_property_table(df, output_path),
        "time": write_time_table(df, output_path),
        "sale": write_sale_table(df, output_path),
    }
    return tables


def read_ppd_table(
    spark: SparkSession, input_path: str, headers_path: str
) -> DataFrame:
    headers = _get_ppd_headers(headers_path)
    df = spark.read.csv(input_path, header=False).toDF(*headers)
    df = df.filter("record_status = 'A'")
    return df


def get_postcode_tables(
    spark: SparkSession,
    input_path: str,
    region_names_path: str,
    output_path: Optional[str] = None,
) -> Dict[str, DataFrame]:
    """
    Process Code-Point Open dataset by extracting a postcode dimension table
    and saving it to S3 in the csv format.

    Args:
        spark: Current Spark session.
        input_path: Path to the postcodes dataset file.
        output_path: Path to where the resulting csv files are saved.
        region_names_path: Path to the CSV mapping of the UK administrative codes to postcodes.

    Returns: A dictionary mapping table names to their dataframes.
    """
    df = read_postcodes(spark, input_path)
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


def read_postcodes(spark: SparkSession, input_path: str) -> DataFrame:
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
    return df


def write_postcode_table(
    df: DataFrame,
    region_names: Broadcast,
    countries: Broadcast,
    output_path: Optional[str],
    filename: str = "postcode.csv",
) -> DataFrame:
    """
    Extract a postcode dimension table from the staging data and save it in the csv format.
    Args:
        df: Staging dataframe containing source data.
        region_names: Mapping of region codes to their names.
        countries: Mapping of the initial letters of the country codes to the country name.
        output_path: Path to where the resulting csv files are saved.
        filename: Optional filename of the resulting csv directory in the output path.

    Returns: A postcode dimension dataframe.
    """
    postcode_table = _add_location(df)
    postcode_table = _normalise_postcode(postcode_table)
    postcode_table = _add_country_name(postcode_table, countries)
    postcode_table = _add_region_name(postcode_table, region_names)
    postcode_table = postcode_table.select(
        ["postcode", "district", "country", "longitude", "latitude"]
    ).na.drop()
    if output_path:
        postcode_table.write.csv(f"{output_path}/{filename}")
        logger.info("Saved postcode table")
    return postcode_table


def main():
    """Read data from S3, process the data using Spark, and write it back to S3."""
    spark = create_spark_session()

    get_ppd_tables(spark, PPD_PATH, OUTPUT_DATA, PPD_HEADERS)
    get_postcode_tables(spark, POSTCODE_PATH, OUTPUT_DATA, REGION_NAMES)

    spark.stop()


if __name__ == "__main__":
    main()


# Utilities:


def _get_ppd_headers(headers_path: str) -> List[str]:
    """
    Extract table headers from the PPD dataset description.
    Args:
        headers_path: Path to the PPD dataset description.

    Returns: An ordered list of headers.
    """
    headers_df = pd.read_csv(headers_path, sep="\t")
    headers = list(
        map(
            lambda col: col.strip().lower().replace(" ", "_").replace("/", "_"),
            headers_df["Data item "],
        )
    )
    return headers


def _normalise_address(df: DataFrame) -> DataFrame:
    """Concatenate street, primary and secondary lines into a single address."""
    return df.withColumn(
        "property_address",
        f.udf(
            lambda paon, saon, street: f"{street}; {paon}; {saon}"
            if saon
            else f"{street}; {paon}",
            t.StringType(),
        )(f.col("paon"), f.col("saon"), f.col("street")),
    )


def _normalise_postcode(df: DataFrame) -> DataFrame:
    """Remove whitespaces from postcodes to ensure consistency."""
    df = df.where(f.col("postcode").isNotNull())
    df = df.withColumn(
        "postcode",
        f.udf(lambda code: code.strip().replace(" ", "").upper())(f.col("postcode")),
    )
    return df


def _add_country_name(df: DataFrame, countries: Broadcast) -> DataFrame:
    """Add a column with a country name based on the provided dictionary."""
    df = df.withColumn(
        "country",
        f.udf(lambda country_code: countries.value[country_code[0]])(
            f.col("country_code")
        ),
    )
    return df


def _add_region_name(df: DataFrame, region_names: Broadcast) -> DataFrame:
    """Add a column with a region name based on the provided dictionary."""
    df = df.withColumn(
        "district",
        f.udf(lambda code: region_names.value.get(code, None))(
            f.col("admin_district_code")
        ),
    )
    return df


def _add_location(df: DataFrame) -> DataFrame:
    """Convert BNG location (eastings and northings) into longitude and latitude."""
    df = df.withColumn("eastings", df["eastings"].cast(t.IntegerType()))
    df = df.withColumn("northings", df["northings"].cast(t.IntegerType()))
    latitude = f.udf(lambda east, north: _bng_to_latlon(east, north), t.FloatType())
    longitude = f.udf(
        lambda east, north: _bng_to_latlon(east, north, lat=False), t.FloatType()
    )
    df = df.withColumn("latitude", latitude(f.col("eastings"), f.col("northings")))
    df = df.withColumn("longitude", longitude(f.col("eastings"), f.col("northings")))
    df = df.where(
        f.col("latitude").isNotNull()
    )  # clean up values that couldn't be converted
    return df


def _bng_to_latlon(east: int, north: int, lat=True) -> Optional[float]:
    try:
        res = utm.to_latlon(east, north, 30, "U")
        return float(res[0]) if lat else float(res[1])
    except ValueError as e:
        return
