# Project: Data Warehouse

# Project Description
The goal of the project is to extract data from AWS S3 to **Amazon Redshift** within two staging tables. From these staging tables, we create a set of fact and dimensional tables.

We know that every time a user of the music app plays a song, it is recorded in the JSON files within the folder `log_data` located in AWS S3 at `s3://udacity-dend/log_data`.
We also know that the information related to each song available on the music app is stored in the JSON files within the folder `song_data` located in AWS S3 at `s3://udacity-dend/song_data/A/A/A`.

# Database Design

- **The staging table `staging_songs`:** Each row holds information related to a JSON file located in `log_data` (AWS S3).
- **The staging table `staging_events`:** Each row holds information related to a JSON file located in `song_data` (AWS S3).

![Star Schema](star_schema.png)

- **The dimension table `songs`:** Holds information about songs available in the music app.
  - **Sort Key:** `song_id`
  - **Distribution:** `All`

- **The dimension table `artists`:** Holds information about artists who created songs available in the music app.
  - **Sort Key:** `artist_id`
  - **Distribution:** `All`

- **The dimension table `time`:** Holds timestamps related to when users played songs.
  - **Sort Key:** `start_time`
  - **Distribution:** `All`

- **The dimension table `users`:** Holds information about users who have played at least one song.
  - **Sort Key:** `user_id`
  - **Distribution:** `All`

- **The fact table `songplays`:** Holds records of song play events.
  - **Sort Key & Dist Key:** `start_time`

# ETL Process
The song-related information is stored in JSON files in the folder `song_data`. Each file corresponds to a specific song. The program extracts this information into the `songs` and `artists` tables.

Every time a user plays a song, it is recorded in the JSON files within `log_data`. Each file represents one day from **November 2018**. The program extracts this data and inserts it into the `time` and `users` tables.

To populate the `songplays` table, the program joins `log_data` with the `songs` and `artists` tables to retrieve `song_id` and `artist_id`.

# Project Repository Files

### **create_tables.py**
- Connects to the **Redshift** cluster `redshift-cluster-1`.
- Creates the empty staging tables: `staging_songs` and `staging_events`.
- Creates the empty fact and dimension tables: `songs`, `artists`, `time`, `users`, and `songplays`.

### **etl.py**
- Connects to the **Redshift** cluster `redshift-cluster-1`.
- Loads data from JSON files in `song_data` into `staging_songs`.
- Loads data from JSON files in `log_data` into `staging_events`.
- Creates the fact and dimension tables from `staging_songs` and `staging_events`.

### **sql_queries.py**
- Contains all SQL queries used by `create_tables.py` and `etl.py`.

# How to Run the Project
To execute the ETL process:
1. Run `create_tables.py` to create the necessary tables.
2. Run `etl.py` to extract, transform, and load the data into Redshift.
