#Unpivot a dataframe dynamically

l = []
seperator = ","

simcols = simulations.select([c for c in df.columns if c not in {'time'}])

A = simcols.columns
B = seperator.join(A).split(",")
n = len(A)

for a in range(len(A)):
  l.append("'{}'".format(A[a]) + "," + B[a])

k = (seperator.join(l))

simulationdf = spark.sql("Select time, stack({0}, {1}) as (Simulation, Sales) from Simulations_Stage".format(n, k)).where("Sales is not null")

#Pivot Dataframe

pivotDF = summarydf.groupBy("Simulation").pivot("DateTime").sum("Sales")