# Data Warehouse for Sparkify

This project aims at creating data warehouse for `Sparkify` startup company. This data warehouse will provide ad-hoc analytical capabilities to analyze songplay data by their users.

## Table of Contents

- [Technologies used](#Technologies-used)
- [Source data information](#Source-data-information)
- [Data Model](#Data-Model)
- [ETL Processing](#ETL-Processing)
- [Project Files](#Project-Files)
- [Sample Data](#Sample-Queries-on-Sparkify)
- [References](#References)

## Technologies used

- Python
- AWS Redshift
- AWS S3

## Source data information

There are two set of source files available for processing. `song_data` and `log_data`.
both the datasets are present on AWS S3 buckets.
song_data is present at 's3://udacity-dend/song_data'
log_data is present at 's3://udacity-dend/log_data'

### song_data
set of files that contains metadata about songs.
- single file contains metadata about a single song.
- json format.
- sample data:

![Song Data Sample](https://www.lucidchart.com/publicSegments/view/edf0ddcc-868e-42a5-8942-bfad22f20fc0/image.png)


### log_data
set of files that contains transactional data about the songs played in the app by various users.
- single file contains transactional data for a given day. a list of json data records are present in the file.
- json format.
- sample data:

![Log Data Sample](https://www.lucidchart.com/publicSegments/view/a60a0666-5cf8-4ab8-a575-b6b64bb6c598/image.png)


## Data Model
    Data is modeled in form of STAR schema tables in AWS Redshift .
    `SPARKIFYDB` database contains the below tables:

    `SONGPLAYS` table is a Fact table containing all the songs played by various users of the app.
    `TIME`,`SONGS`,`USERS` and `ARTISTS` tables are Dimentsion tables containing metadata about the songplay data.
    
Below is the ER diagram which provides more details about the database tables:
![Sparkify Relational Data Model](https://www.lucidchart.com/publicSegments/view/6b022f3c-4036-4f3a-8855-40d502463dc3/image.jpeg)

## ETL Processing

### Data Profile:

* songs and artists tables are populated from the `song_data` source file. 
* Data profiled shows that `year, artist_location, artist_latitude and artist_longitude` are not always present. 
* An Artist could have multiple songs. some song_names have special characters in the source file encoded in utf8 format and target database is set to the same in order to preserve data accuracy. 
* Users, time and songplay tables are populated from the `log_data` source file(except for user_id and artist_id which are linked from songs and artists table respectively).
* A user could have multiple songplays.

### Data Processing
* Data from both the datasets (log_data and song_data) are loaded into staging tables `events` and `songs_raw` respectively in AWS Redshift.
* COPY command is used to copy data directly from json files to Redshift Staging tables.
* jsonpath file is provided in the COPY command for the events data. Also appropriate timestamp format is provided for copying.
* sql queries are used to insert data into the target tables from the staging tables and are invoked from the python script using the psycopg2 package.

### Data Distribution for tables in Redshift:

* `songplays` table is the fact table and is distributed and sorted by start_time which provides a uniform distribution of data and also provides data locality when joining with the `time` table.
* `time` table is distributed and sorted by start_time which provides a uniform distribution of data.
* `users` table data distribution is set to auto.
* `artists` table is distributed by artist_id which provides a uniform distribution of data.
* `songs` table is distributed by song_id which provides a uniform distribution of data.

## Project Files
    `create_tables.py` and `sql_queries.py` are used to create the required database schema as per the model and has insert queries for the respective tables.
    `etl.py` is used for using the source JSON files and storing the data in staging tables in redshift and then moving data to the target tables in the sparkify database in redshift.
    `aws_cluster_setup.py` is used to create a redshift cluster and a IAM role which has the required access for reading S3 data and creating and inserting into tables in redshift database.
    `dwh-aws-setup.cfg` and `dwh.cfg` contain configuration access keys for AWS and other information regarding file location on S3, etc.

## Sample Queries on Sparkify
Below Query shows top 5 hours where the most number of songs are played.

![](top-5-hours-songplay.PNG)

Below Query shows top 5 users monthwise. (As the data is present for 2018-11 only - that is what we see in the output)
![](top-5-users.PNG)

## References

* https://docs.amazonaws.cn/en_us/redshift/latest/dg/c_best-practices-best-dist-key.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html
* https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-blanksasnull
* https://pandas.pydata.org/pandas-docs/stable/reference/index.html
* https://www.lucidchart.com