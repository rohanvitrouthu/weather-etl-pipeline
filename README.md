# weather-etl-pipeline
A simple ETL pipeline for extracting Weather data from Open Meteo API, performing basic data cleaning and transformations, and storing the transformed data on a PostgreSQL database.

This project was done to understand the very basics of Data Engineering which includes extracting data from an API, cleaning and transforming it, and pushing it to a target storage location such as a Database table.

Challenges faced:
1. Database passwords that have special characters: I used quote_plus() function from urllib.parse library to parse the database password. Though it worked on Python, when I was trying to connect to the 'weatherdb' database through psql CLI on pgAdmin4 on my Windows 11 PC using the username and password, the password was not being parsed properly. Of course there are ways to overcome that, but that was not the main focus and a lot of time was getting spent on trying to fix it, so I decided to modify the password to not have special characters and everything went fine.