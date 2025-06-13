# Project 2: Data warehouse with AWS Redshift


# Purpose

This project serves to assist the Sparkify company in moving their data and processes to an AWS cloud data warehouse.  
They can use it as a tool to scale up their business, reduce cost, increase efficiency, cross-colaborations, and analytics.


# Database schema design

Sparkify's streaming service generates a great volume of data. Every department in the company can benefit from the information about their users' behaviour and choices. That's why a star schema is a logical choice for the database schema design. It is easy to design, and more efficient in querying because of fewer JOINs.

The data resides in S3, in 2 directories of JSON logs [song_data, log_data].  
From there, it needs to move to a Redshift cluster, to 2 staging tables [staging_events, staging_songs].  
As the database schema design follows star schema, the 2 staging tables will inject their data into 1 fact table [songplays] and 4 dimension tables [users, songs, artists, time].  


## ETL pipeline and file overwiev

The pipeline consists of three python scripts and one configuration file:
1. sql_queries.py contains queries to DROP tables if they exist, CREATE them, COPY data into the 2 staging tables, and INSERT it into the remaining 5 tables. 
2. create_tables.py imports the DROP and CREATE queries from the sql_queries.py, and it contains 2 functions to connect to the Redshift cluster and execute these queries.
3. etl.py imports the COPY and INSERT queries from the sql_queries.py, and it contains 2 functions to connect to the Redshift cluster and execute these queries.
4. dwh.cfg contains values that the two scripts will import and use to create a connection string to connect to Redshift. 

You can run the scripts in your terminal app, by running the create_tables.py first, then the etl.py.

In addition to the aforementioned content, the sql_queries.py also includes 3 queries to return:
1. top 5 most played artists
2. which gender (female, male) listens to more songs
3. top 5 listening peak hours.

You don't need to do anything to get answers to these questions. The etl.py script will import and run them after the load is done.

If you wish to experiment with the code, you can copy-paste the scripts' content into an enviroment to your liking, such as Jupyter notebook or VS Code file, and run it from there.


# Troubleshoot

Whether you wish to only run these scripts, or even recreate or continue this project, you may experience several difficulties.  

## Pause your cluster

Sparkify's budget for this project was 25$ budget and their advice was to create a cluster with 4 nodes. For their amount of data and my level of Data Engineering expertise, the budget proved to be enough. If you are an aspiring Data Engineer in training and wish to recreate this project, for your info:
1. The amount of data in this project is 32,171 rows.
2. The first half of the project, I created/used a 4-node cluster.
3. One night I forgot to pause my cluster and burned through half the budget, so I switched to a single-node cluster.
4. This is my 2nd month of Udacity Data Engineering with AWS course/nanodegree, and my 2nd project.
5. 1 year before this course, I learned the foundations of coding in Python and SQL.

Conclusion: 25$ is an adequate budget. A single node costs 0.25 per hour. Pause your cluster accordingly.

## Know the requirements

The main working environment for this project and its prep lessons is Jupyter notebook hosted on Udacity's online Workspace platform.  
If you prefer working on your local host, ask your mentor for the system requirements.  
There were 2 major issues I experienced and overcome in Jupyter notebook on my localhost. To spare you dancing in the dark, I combed through the Udacity's Knowledge platform, where other students report their problems and ask for help. There are many reports of these 2 issues, and there were many different solutions offered by the mentors. None of those solutions worked for me. Here's what did.

### KeyError: 'DEFAULT'

Lesson 2, Introduction to data warehouses, you will work in ipython-sql and use %sql magic commands to connect to your local Postgres database.  
If the starter code throws the **KeyError: 'DEFAULT'**,  do this:
- in the line under the %sql $conn_string, add **%config SqlMagic.style = "_DEPRECATED_DEFAULT"**.
- restart kernel and re-run the cell.

### OperationalError: SSL error

This error had occured on 3 different attempts to create a connection to Redshift cluster:
1. In the Lesson 4, AWS data warehouse technologies, will require you to create a postgresql connection.
2. In this project, 2 scripts [create_tables.py, etl.py] will attempt to create a psycopg2 connection.

Your localhost may fail to connect to Redshift and throw the following error:
   ```
OperationalError: connection to server at "...redshift.amazonaws.com" failed: SSL error: certificate verify failed
FATAL: no pg_hba.conf entry for host "::ffff:your.ip.address", user "dwhuser", database "dwh", SSL off
   ```
A single solution worked for me to resolve all 3 of them.

#### The recommended solution has 2 steps:

1. read about and download the [Amazon Redshift SSL certificate](https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-ssl-support.html)
2. in the Lesson 4 exercise, change the postgresql connection string from:
   ```
   conn_string = postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
   ```
to:
   ```
    conn_string = f"postgresql://{dwh_db_user}:{dwh_db_password}@{dwh_endpoint}:{dwh_port}/{dwh_db}?sslmode=verify-full&sslrootcert=system"
   ```

3. in this project scripts, change the psycopg2 connection string from:
   ```
   conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
   ```
to:
   ```
   conn = psycopg2.connect(
    "host={} dbname={} user={} password={} port={} sslmode=verify-full sslrootcert=system".format(*config['CLUSTER'].values())
)
   ```

#### The less secure alternative is to bypass SSL verification locally.

Only use this as a last resort, a temporary measure, and don't make this a habit! In the create_tables.py and etl.py scripts, change the psycopg2 connection string from:
   ```
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
   ```
to:
   ```
    conn = psycopg2.connect(
    "host={} dbname={} user={} password={} port={} sslmode=require sslrootcert=invalid".format(*config['CLUSTER'].values())
)
   ```

### Datatype compatibility

If you ever get to update or change this code, for example if you are Sparkify's data engineer wishing to scale up the database or create new tables, be wary of compatibility between the desired datatype in the table and the actual data. Incompatible data will throw errors and add up time to create new tables. Luckily, those errors are usually very informative. Here are the common conflicts I experienced:

1. ```Error: "Missing data for not-null field"```
   - Meaning: the column contains missing data, and the table's column was created with the NOT NULL constraint.
   - Solution: this error occured in many columns, so it's best to avoid the NOT NULL constraint when creating tables.

2. ```Error: "String length exceeds DDL length"```
   - Meaning: the column contains some values with more characters than you allowed in the VARCHAR() datatype.
   - Solution: use datatype VARCHAR(MAX), but use it wisely and only where necessary because it's a trade-off with memory consumption.

3. ```Error: "Overflow, Column type: Integer"```
   - Meaning: the column contains some values/numbers bigger than the datatype INT can hold.
   - Solution: use datatype BIGINT.
