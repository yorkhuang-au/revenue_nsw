# Coding Task for Revenue NSW

## Assumption and Decision
Some assumptions and decisions have been made by the developer due to time limit and understanding.

1. The input birth date format is in dmmyyyy instead of yyyy-mm-dd as noted in the requirements.
2. The salary is not rounded to whole dollar as I understand it is required to format with "$" prefix and comas.
   I also round the salary to 4 decimal places.
3. The requirement indicates to create a list of dict in memory. It's assumed the data size should fit in memory.
   No distributed multi-nodes framework, such as Spark, is used in this task.

## How to build and run.
This requires Docker Desktop and docker compose are installed.

The shell scripts provied are tested in Ubuntu, Mac OS and Windows WSL.
In Windows, please install WSL (Windows Subsystem) and Ubunut.

If there is any issue in running the shell scripts, you can run the
commands in shell scripts directly.

For Windows, please use WSL(Windows Subsystem for Linux) and Ubuntu.

### Clone the repository from github
```
git clone https://github.com/yorkhuang-au/revenue_nsw.git
cd revenue_nsw
```

### Unit Test
In the revenue_nsw/ folder, run unit test as below.

```
./unittest.sh
```

### Run ETL
In the cd revenue_nsw/ folder, run etl as below.

All data files must be in revenue_nsw/app/data folders.

```
./etl.sh /data/<data-file1> /data/<data-file2> ...
```

For example, I have added 2 data files with exactly the same contents as the data file provided.
```
revenue_nsw/app/data/member-data.csv
revenue_nsw/app/data/member-data-copy.csv
```
To run etl for them, run below.
```
./etl.sh /data/member-data.csv /data/member-data-copy.csv
```
This will load data into MongoDB created by the etl.sh script.

### MongoDB

MongoDB configuration:
* User name is: root
* Password is: example
* Database: revenue_db
* Collection: member_data

The mongo service is started after running etl.sh or mongo.sh for the first time. It runs in detached mode until stopped by user.

To go to mongosh, run below.
```
./mongosh.sh
```
In mongosh prompt, you can run mongosh commands.
For example, to find out the number of records in our member_data collection, run below from mongosh prompt.
```
use revenue_db
db.member_data.find().size()
```

### Rebuild etl image
If you change the etl code and want to rebuild the image for etl.sh, run below.
```
./rebuild_etl.sh
```

### Stop and Clean up
Stop MongoDB without deleting the container, run below in revenue_nsw/ folder.
```
./stop_mongo.sh
```

You don't need to explicitly restart the mongodb. It will check and restart in etl.sh and mongosh.sh.

To clean up all the container and network, run below.

```
./cleanup.sh
```
