fig, ax = plt.subplots(figsize=(10,6))
gr1 = sns.lineplot(data=log_returns)
display(gr1)

%fs ls "/FileStore/"

%fs rm -r "/FileStore/import-stage"

#Create image and save to blob
plt.subplots(figsize=(10,6))
sns.lineplot(data=log_returns)
plt.savefig('/dbfs/FileStore/import-stage/gr2.png')
dbutils.fs.cp('dbfs:/FileStore/import-stage/gr2.png', "/mnt/global-cube")

%fs ls "mnt/global-cube"