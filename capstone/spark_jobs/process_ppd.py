from etl_spark import (OUTPUT_DATA, PPD_HEADERS, PPD_PATH,
                       create_spark_session, get_ppd_tables)

spark = create_spark_session()
get_ppd_tables(spark, PPD_PATH, OUTPUT_DATA, PPD_HEADERS)
spark.stop()
