#Read JSON data
mwDF = spark.read.json("/mnt/test1/m_w.json")
mwDF.printSchema()


#Store schema as object
mwSchema = mwDF.schema
print(type(mwSchema))

[field for field in mwSchema]


#UDF Schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

mwSchema2 = StructType([
  StructField("state", StringType(), True), 
  StructField("price", IntegerType(), True) 
])


#Read JSON data and apply UDF schema

mw2 = (spark.read
  .schema(mwSchema2)
  .json("/mnt/test1/m_w.json")
)

display(mw2)


#Primitive, Non-Primitive UDF Schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType

mwSchema3 = StructType([
  StructField("state", StringType(), True), 
  StructField("coordinates", 
    ArrayType(FloatType(), True), True),
  StructField("price", IntegerType(), True)
])


mw3 = (spark.read
  .schema(mwSchema3)
  .json("/mnt/test1/m_w.json")
)

display(mw3)
