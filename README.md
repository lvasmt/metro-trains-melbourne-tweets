# metro-trains-melbourne-tweets

*Note:* This dashboard is no longer being populated with new data due to the Twitter API access being restricted. 

This is a repository containing code used for the Metro Trains Melbourne Tweets analysis dashboard. 

In this project, I built a data pipeline and a dashboard to visualise Metro Trains Melbourne’s Tweets and their contents.  The dashboard can be found at this link: https://lookerstudio.google.com/reporting/9b762e3d-fc56-4911-9ac7-a7a04c94d180. 

- The project was deployed using Google Cloud Functions for the Python code that extracts the data from Twitter's API and loads it up to Google BigQuery. 
- Google Cloud Scheduler was used to trigger the Cloud Function daily at 11am Melbourne time. 
- dbt was then used to deduplicate and transform the data that was stored in Google BigQuery into the final tables to be used for visualisation. 
- Google Looker Studio was used to categorise the tweets by using regular expressions and case functions. 
- Google Looker Studio was finally used to visualise the data on the dashboard. 
