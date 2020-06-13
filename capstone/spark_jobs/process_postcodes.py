from etl_spark import (OUTPUT_DATA, POSTCODE_PATH, REGION_NAMES,
                       create_spark_session, get_postcode_tables)

spark = create_spark_session()
get_postcode_tables(spark, POSTCODE_PATH, OUTPUT_DATA, REGION_NAMES)
spark.stop()
