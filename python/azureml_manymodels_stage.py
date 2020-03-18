from pyspark.sql.functions import col, lit, unix_timestamp, datediff, to_date, from_unixtime, max, min, sum, count, explode, year, month, concat, lower, upper, approx_count_distinct, first
from pyspark.sql.types import *
from pyspark.sql.window import Window
import seaborn as sns
import matplotlib.pyplot as plt
from pandas import melt
from pyspark.sql.types import DateType


df = spark.read.parquet("/mnt/forecast/ts/tssourcedata")


df1 = (df.select('Year','Month','Country','Product','Units')
       .withColumn('DateTime',unix_timestamp(concat(col('Year'),lit('-'),col('Month'),lit('-01')),'yyyy-MM-dd')
      ))


df1 = df1.withColumn('Date',df1['DateTime'].cast(TimestampType())).drop('DateTime')
df1 = df1.withColumn('Country_Item',concat(col('Country'),lit('-'),col('ItemID')))


#create train, test dataframes
dftrain = df1.select("*").where(col("Year") < 2018)
dftest = df1.select("*").where(col("Year") >= 2018)


dftrain.repartition("Country_Item").write.partitionBy("Country_Item").mode('overwrite').option("header","true").format("csv").save('/mnt/forecast/ts/azuremldata/train')
dftest.repartition("Country_Item").write.partitionBy("Country_Item").mode('overwrite').option("header","true").format("csv").save('/mnt/forecast/ts/azuremldata/test')