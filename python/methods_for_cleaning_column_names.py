#Read data

maxDF= (spark.read
  .option("delimiter", "\t")
  .option("header", True)
  .option("timestampFormat", "mm/dd/yyyy hh:mm:ss a")
  .option("inferSchema", True)
  .csv("/mnt/test1/m_w.csv")
)
display(maxDF)

#Rename columns in maxDF, no spaces or invalids, set as camelcase

cols = maxDF.columns
titleCols = [''.join(j for j in i.title() if not j.isspace()) for i in cols]
camelCols = [column[0].lower()+column[1:] for column in titleCols]

maxDFrenamed = maxDF.toDF(*camelCols)
display(maxDFrenamed)


#Alternative to remove spaces from columns and use lower case

for c in maxDF.columns:
  maxDFrenamed2 = maxDF.withColumnRenamed(c, c.replace(' ','').lower())