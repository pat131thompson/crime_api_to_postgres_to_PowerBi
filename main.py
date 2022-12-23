# Relevant module imports
import glob
import numpy
import pandas
import psycopg2
import requests

"""
---------
API LINKS
---------
A twelve month period of API calls to data.police.uk requesting all crime data by month from a mile radius of 
central Wymondham.  Lat and long co-ords provide central point of mile radius.  Lat / Long can be changed at will.
"""
list_of_months = ['https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-01',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-02',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-03',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-04',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-05',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-06',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-07',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-08',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-09',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-10',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-11',
                  'https://data.police.uk/api/crimes-street/all-crime?lat=52.629478&lng=1.300965&date=2021-12'
                  ]

"""
--------------
CAPTURING DATA 
--------------
1. For each month in list_of_months - identifies index position
2. Creates a filename of "data_save" plus index position of month
3. Checks GET request response -- if 200;
4. JSON data returned - stored in data_return variable
5. PANDAS dataframe variable created and normalises data from JSON
6. That dataframe is then saved to a CSV file and tagged with the filename created in (2)
"""
for month in list_of_months:
    index_num = list_of_months.index(month)
    filename = f"data_save_{index_num}.csv"
    response = requests.get(month)
    if response.status_code == 200:
        data_return = response.json()
        data_frame = pandas.json_normalize(data_return)
        data_frame.to_csv(filename)
    else:
        print('Error:', response.status_code)

"""
------------------
MERGE & CLEAN DATA
------------------
1. Identifies all .csv files in working directory and places them into a list.
2) Creates three "all_data" files (crime, outcome & location) with a view to then clean each to make consistent with DB.
3) .drop methods are called for each three removing redundant columns that will error on DB load later in code.
4) Longitude data is converted to absolute values for correct positioning in map
5) all_data files x 3 are then saved to a .csv in the working directory.
6) This ensures from a records perspective I can show both raw data from API calls and subsequent cleansed data into DB.
"""
csv_list = glob.glob('*.{}'.format('csv'))
all_data_crime = pandas.concat([pandas.read_csv(csv) for csv in csv_list])
all_data_outcome = pandas.concat([pandas.read_csv(csv) for csv in csv_list])
all_data_location = pandas.concat([pandas.read_csv(csv) for csv in csv_list])

# clean data from all_data_crime
all_data_crime.drop('location_type', inplace=True, axis=1)
all_data_crime.drop('context', inplace=True, axis=1)
all_data_crime.drop('outcome_status', inplace=True, axis=1)
all_data_crime.drop('persistent_id', inplace=True, axis=1)
all_data_crime.drop('location_subtype', inplace=True, axis=1)
all_data_crime.drop('location.latitude', inplace=True, axis=1)
all_data_crime.drop('location.street.id', inplace=True, axis=1)
all_data_crime.drop('location.street.name', inplace=True, axis=1)
all_data_crime.drop('location.longitude', inplace=True, axis=1)
all_data_crime.drop('outcome_status.category', inplace=True, axis=1)
all_data_crime.drop('outcome_status.date', inplace=True, axis=1)

# clean data from outcome
all_data_outcome.drop('category', inplace=True, axis=1)
all_data_outcome.drop('location_type', inplace=True, axis=1)
all_data_outcome.drop('context', inplace=True, axis=1)
all_data_outcome.drop('outcome_status', inplace=True, axis=1)
all_data_outcome.drop('persistent_id', inplace=True, axis=1)
all_data_outcome.drop('location_subtype', inplace=True, axis=1)
all_data_outcome.drop('month', inplace=True, axis=1)
all_data_outcome.drop('location.latitude', inplace=True, axis=1)
all_data_outcome.drop('location.street.id', inplace=True, axis=1)
all_data_outcome.drop('location.street.name', inplace=True, axis=1)
all_data_outcome.drop('location.longitude', inplace=True, axis=1)

# clean data from all_data_location
all_data_location.drop('category', inplace=True, axis=1)
all_data_location.drop('location_type', inplace=True, axis=1)
all_data_location.drop('context', inplace=True, axis=1)
all_data_location.drop('outcome_status', inplace=True, axis=1)
all_data_location.drop('persistent_id', inplace=True, axis=1)
all_data_location.drop('location_subtype', inplace=True, axis=1)
all_data_location.drop('month', inplace=True, axis=1)
all_data_location.drop('outcome_status.category', inplace=True, axis=1)
all_data_location.drop('outcome_status.date', inplace=True, axis=1)
all_data_location['location.longitude'] = numpy.abs(all_data_location['location.longitude'])

# clean data to CSV
all_data_crime.to_csv("all_data_crime.csv", index=False)
all_data_outcome.to_csv("all_data_outcome.csv", index=False)
all_data_location.to_csv("all_data_location.csv", index=False)

"""
-----------------
DATABASE CREATION
-----------------
1) Opens the pw.txt file in working directory and saves it to the password_file variable.
   password variable created and reads the data from the password_file (which is then closed).
2) Connects to local instance of PostgreSQL
3) Creates SCHEMA and sets search path to PUBLIC
4) DDL creation of x 3 tables
"""
password_file = open("pw.txt", "r")
password = password_file.read()
password_file.close()
connect = psycopg2.connect(
    database="postgres",
    user="postgres",
    password=password,
    host="localhost",
    port="5432"
    )
connect.autocommit = True
cursor = connect.cursor()
schema = "CREATE SCHEMA crime_data"
cursor.execute(schema)
search_path = "SET SEARCH_PATH TO crime_data, PUBLIC"
cursor.execute(search_path)
crime_table = "CREATE TABLE crime (" \
              "index_num INTEGER, " \
              "category VARCHAR (255) NOT NULL CHECK (category <> ''), " \
              "id DECIMAL (10,1) PRIMARY KEY, " \
              "month VARCHAR (7) NOT NULL" \
              ")"
cursor.execute(crime_table)
outcome_table = "CREATE TABLE outcome (" \
                "index_num INTEGER, " \
                "id DECIMAL (10,1) REFERENCES crime (id), " \
                "outcome_status_category VARCHAR (255), " \
                "outcome_status_date VARCHAR (7)" \
                ")"
cursor.execute(outcome_table)
location_table = "CREATE TABLE location (" \
                 "index_num INTEGER, " \
                 "id DECIMAL (10,1) REFERENCES crime (id), " \
                 "location_latitude DECIMAL (8,6) NOT NULL, " \
                 "location_street_id DECIMAL (8,1) NOT NULL, " \
                 "location_street_name VARCHAR (255) NOT NULL, " \
                 "location_longitude DECIMAL (8,6) NOT NULL" \
                 ")"
cursor.execute(location_table)

"""
----------------
DATABASE LOADING
----------------
1) The three "all_data" .csv files are loaded to their corresponding DB tables.
"""
with open('all_data_crime.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'crime', sep=',', columns=('index_num', 'category', 'id', 'month'))
connect.commit()
with open('all_data_outcome.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'outcome', sep=',', columns=('index_num', 'id', 'outcome_status_category',
                                                     'outcome_status_date'))
connect.commit()
with open('all_data_location.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'location', sep=',', columns=('index_num', 'id', 'location_latitude', 'location_street_id',
                                                      'location_street_name', 'location_longitude'))
connect.commit()
