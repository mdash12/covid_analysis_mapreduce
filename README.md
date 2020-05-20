# MapReduce in Pseudo-distributed Hadoop and Spark

### Task 1:
Count the total number of reported cases for every country/location till April 8th, 2020

Program arguments description
 - The HDFS path to your input data file
 - [true | false] Include "World" data in the result or not. True will include total number of reported cases for "World" in the result, False will ignore the rows with location/country = world
 - The HDFS output path for your program. (NOTE: You should remove the output path after every execution of your program. Hadoop cannot start a job if output directory is already created)

### Task 2:
Modify your program to report total number of deaths for every location/country in between a given range of dates.

Program arguments description
 - The HDFS path to your input data
 - Start date (YYYY-MM-DD)
 - End date (YYYY-MM-DD)
 - The HDFS output path for your program. (NOTE: You should remove the output path after every execution of your program. Hadoop cannot start a job if output directory is already created)

### Task 3:
Output the total number of cases per 1 million population for every country
```sh
(total_number_of_country_cases/country_population) * 1,000,000
```
Program arguments description
- The HDFS path to your input data file (covid19_full_data.csv)
- The HDFS path to populations.csv
- The HDFS output path for your program. (NOTE: You should remove the output path after every execution of your program. Hadoop cannot start a job if output directory is already created)
- Add population.csv file to Hadoop DistributedCache

### Dataset
Dataset Description
The dataset contains COVID-19 case information from the start of this year till April 8, 2020 for most of the countries of the world in .csv (comma separated value). Each line consists of number of new cases, new deaths, total number of cases and total deaths for a given country on given day. The data format is as follows
```sh
[date],[location/country],[new_cases],[new_deaths]
```
The second dataset, required for task-3, consists of population information for world countries as of the year 2020. The format of the data is as follows
```sh
[country],[location],[continent],[population_year],[population]
```

#### Dataset Download
You can download sample dataset using following terminal commands

(covid19 dataset for task-1, task-2 and task-3)
```sh
wget http://bmidb.cs.stonybrook.edu/publicdata/cse532-s20/covid19_full_data.csv
hdfs dfs -mkdir -p /cse532/input
hdfs dfs -put covid19_full_data.csv /cse532/input/
```

(population dataset for task-3)
```sh
wget http://bmidb.cs.stonybrook.edu/publicdata/cse532-s20/populations.csv
hdfs dfs -mkdir -p /cse532/cache
hdfs dfs -put populations.csv /cse532/cache/
```

### Execution
To execute Hadoop jobs using Java API (Terminal Commands)
```sh
hadoop Covid19.jar Covid19_1 /cse532/input/covid19_full_data.csv true /cse532/output/
hadoop Covid19.jar Covid19_1 /cse532/input/covid19_full_data.csv false /cse532/output/
hadoop Covid19.jar Covid19_2 /cse532/input/covid19_full_data.csv 2020-01-01 2020-03-31 /cse532/output/
hadoop Covid19.jar Covid19_3 /cse532/input/covid19_full_data.csv cse532/input/populations.csv /cse532/output/
```
##### To view output (Terminal Commands): 
List contents of HDFS output directory
```sh
hdfs dfs -ls /cse532/output/
```
Print out the contents of output files to terminal
```sh
hdfs dfs -cat /cse532/output/part-*
```

##### To execute Spark jobs using Python API (Terminal Commands)

Submit Spark jobs
```sh
spark-submit SparkCovid19_1.py /cse532/input/covid19_full_data.csv 2020-01-01 2020-03-31 /cse532/output/
spark-submit SparkCovid19_2.py /cse532/input/covid19_full_data.csv cse532/input/populations.csv /cse532/output/
```

