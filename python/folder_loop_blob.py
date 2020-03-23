filespath = "dbfs:/mnt/forecast/ts/azuremldata/training_data_germany_all_folders/"
#files = list(map(lambda x: x.path, dbutils.fs.ls(filespath)))
#csvfiles = [file for file in files if '.csv' in file]
folders = list(map(lambda x: x.path, dbutils.fs.ls(filespath)))

new_path = 'mnt/forecast/ts/azuremldata/training_data_all_germany_all/'

for folder in folders:
    files = list(map(lambda x: x.path, dbutils.fs.ls(folder)))
    csvfiles = [file for file in files if '.csv' in file]
    for file in csvfiles:
        #target_filename = file.replace(filespath,new_path)
        filename = dbutils.fs.ls(file)[0].name
        #for filename in filenames:
        #print(filename)
        dbutils.fs.cp(file, new_path + filename)
        
print('DONE Training')
        
        
filespath = "dbfs:/mnt/forecast/ts/azuremldata/testing_data_germany_all_folders/"
#files = list(map(lambda x: x.path, dbutils.fs.ls(filespath)))
#csvfiles = [file for file in files if '.csv' in file]
folders = list(map(lambda x: x.path, dbutils.fs.ls(filespath)))

new_path = 'mnt/forecast/ts/azuremldata/testing_data_all_germany_all/'

for folder in folders:
    files = list(map(lambda x: x.path, dbutils.fs.ls(folder)))
    csvfiles = [file for file in files if '.csv' in file]
    for file in csvfiles:
        #target_filename = file.replace(filespath,new_path)
        filename = dbutils.fs.ls(file)[0].name
        #for filename in filenames:
        #print(filename)
        dbutils.fs.cp(file, new_path + filename)

print('DONE Testing')