%python
#Imports
from pyspark.sql import *
from pyspark.sql.functions import col, udf, concat, lit, abs, round, year, month, dayofmonth, current_date, current_timestamp, unix_timestamp
from pyspark.sql.types import IntegerType, FloatType, DecimalType, StringType, DateType
from pyspark.sql import functions as f


%python 
#Get FX Rates

pushDown_query1 = """(select * from EO_FXRates) as t"""

fx = spark.read.jdbc(url=jdbcUrl, table=pushDown_query1, properties=connectionProperties)

for c in fx.columns:
  fx = fx.withColumnRenamed(c, c.replace(' ','').lower())

#Filter
fx = (fx
      .where(col('owner')== 'site1')
      .select('*')
     )


%scala
val df = spark.read.format("csv")
  .option("sep", ";")
  .option("inferSchema", "false")
  .option("header", "false")
  .load("/FileStore/tables/site1.txt")

%sql
drop table if exists dfmax

df.createTempView("dfmax")


%python

dfmax = sqlContext.table("dfmax")
dfmax = dfmax.select("_c1", "_c6", "_c8", "_c18")

# create a monotonically increasing id 
dfmax = dfmax.withColumn("idx", f.monotonically_increasing_id())

# then since the id is increasing but not consecutive, it means you can sort by it, so you can use the `row_number`
dfmax.createOrReplaceTempView('dfnew')
new_df = spark.sql('select row_number() over (order by "idx") as num, * from dfnew')
df = new_df.filter(col('num') != 1).drop('num','idx')
df = (df
      .withColumnRenamed('_c1', 'column1')
      .withColumnRenamed('_c6', 'column2')
      .withColumnRenamed('_c8', 'column3')
      .withColumnRenamed('_c18', 'column4')
     )

%python 
df = df.withColumn('column2', f.regexp_replace('column2', ',', '.').cast('float'))

%python 
df = df.withColumn('column3', df['column3'].cast(IntegerType()))

%python 
#creating a dataframe to hold the fx_rate 
rate = fx.select('fx_rate').collect()[0]
rate = spark.createDataFrame(rate, FloatType()) 

%python 
#cross joining rate to the the inventory df to run the inventory amount on hand calc with fx_rate adjustment 
df1 = df.join(rate).select('column1', 'column2', 'column3', 'column4', 'column5', (df.column3 * rate.value).alias('column6'))


%python
df1 = df1.withColumn('owner', lit('site1'))

%python
dftest = df1.groupBy('column2').agg(f.sum('column6')).withColumnRenamed('sum(column6)', 'Value')
dftest.withColumn('Total', dftest.Value.cast(DecimalType(18, 2))).drop('Value').show()