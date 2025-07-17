from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("GCP GCS Read & Write to BigQuery") \
      .getOrCreate()
   print("Usecase5: Spark Application to Read from GCS and load to Bigquery (Raw) and load into another Bigquery (Curated) table in the GCP")
   print("1. Ingestion layer data read from GCS - brought by some data producers")
   gcs_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("gs://jayashree-first-bucket/custs").toDF("custno","firstname","lastname","age","profession")
   gcs_df.show()
   print("1. GCS Read Completed Successfully")
   print("Ensure to create the BQ datasets (rawds & curatedds) -> table creation (optional) ")
   print("2. Writing GCS data to Raw BQ table")
   gcs_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider') \
   .option("temporaryGcsBucket",'jayashree-first-bucket/tmp')\
   .option('table', 'rawds.customer_raw') \
   .save()
   print("3. GCS to Raw BQ raw table loaded")
   gcs_df.createOrReplaceTempView("raw_view")
   curated_bq_df=spark.sql("select custno, concat(firstname,',', lastname) as name, age, coalesce(profession,'unknown') as profession from raw_view where age>30")
   print("Read from rawds is completed") 
   curated_bq_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider') \
   .option("temporaryGcsBucket",'jayashree-first-bucket/tmp')\
   .option('table', 'curatedds.customer_curated') \
   .save()
   print("GCS to Curated BQ table load completed")
main()