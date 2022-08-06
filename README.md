## Purpose of the database:

For a song platform to understand what needs to be done in order to improve, the most relevant piece of information to know is how the users behave. 

There are different useful metrics to take decisions like amount of songs listened per day per user. Most listened song. Listened songs per platform. And so on ...

Having a structure database that easily allows the analysis of these metric is essential for the company.

## How to run the scripts:

1. [Download and Install Python](https://www.python.org/downloads/)
2. Create the database by running ```python3 create_tables.py```
3. Run the etl script by running ```python3 etl.py```

## Provided files

#### data
Includes all the song_data and log_data needed to fill the database

#### sql_queries.py
Python file with the queries required to create and handle the database

#### create_tables.py
Python files with the necessary functionality to create the database

#### etl.ipynb
Experimental Jupiter notebook with the steps used to define the etl process

#### etl.py
Python file with the necessary functionality to extract the data from the songs and logs files, transform it and load it into the database

#### test.ipynb
Jupiter notebook with sanity tests

## Schema decision

For this project, the schema implemented for the database was a star.
This decision is tightly related to the purpose of the project which is understanding how the users behave. 
The main analysis will be done only using one songplays table. This is highly efficient for reading because there is no need to do joins with other tables. 

