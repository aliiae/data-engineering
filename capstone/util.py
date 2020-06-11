import logging
from typing import List, Optional

import pandas as pd
import utm
from pyspark import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
        "address",
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
        "postcode", f.udf(lambda code: code.strip().replace(" ", ""))(f.col("postcode"))
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
    if 100000 < east < 999999:
        res = utm.to_latlon(east, north, 30, "U")
        return float(res[0]) if lat else float(res[1])
    return None
