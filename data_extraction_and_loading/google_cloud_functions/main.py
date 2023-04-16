from logging import NullHandler
import requests
import os
import json
import pytz
import pandas as pd 
from google.cloud import bigquery
from google.oauth2 import service_account
from time import sleep
from datetime import datetime, timedelta



#----------- Twitter-related Variables --------------------------
file = open('twitter_api_keys.txt',)
keys = json.load(file)

bearer_token = keys['bearer token']
#the user_id below is the user ID of Metro Trains Melbourne's Twitter account. 
user_id = '40561535'
max_results = 100
pagination_token = 'init'
dtformat = '%Y-%m-%dT%H:%M:%SZ'
start_time_days_ago = 90
end_time_days_ago = None

request_delay = 5


#----------- GCP-related Variables --------------------------

key_path = "gcp_keys.json"
project_id = 'gcp-project'

#----------- Meta data config vars ---------------------------
df_meta_data_columns = pd.DataFrame({'result_count': pd.Series(dtype='int'),
                            'newest_id': pd.Series(dtype='str'),
                            'oldest_id': pd.Series(dtype='str'),
                            'next_token': pd.Series(dtype='str'),
                            'previous_token' : pd.Series(dtype='str')})

table_id_meta_data = "gcp-project.analytics.meta_data_metro_trains_twitter"

job_config_meta_data = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("time_of_load","DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("result_count", "INT64"),
        bigquery.SchemaField("newest_id", "STRING"),
        bigquery.SchemaField("oldest_id", "STRING"),
        bigquery.SchemaField("next_token","STRING"),
        bigquery.SchemaField("previous_token","STRING")
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_APPEND",
)

#----------- Raw data config vars ---------------------------
table_id_raw_data = "gcp-project.analytics.raw_metro_trains_twitter"

job_config_raw_data = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("time_of_load","DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("author_id", "STRING"),
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("edit_history_tweet_ids","STRING"),
        bigquery.SchemaField("created_at","DATETIME"),
        bigquery.SchemaField("in_reply_to_user_id","STRING")
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_APPEND",
)

#----------- Staging data config vars ---------------------------
table_id_stg_data = "gcp-project.analytics.stg_metro_trains_twitter"

job_config_stg_data = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("time_of_load","DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at","DATETIME"),
        bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_APPEND",
)

timezone = pytz.timezone('Australia/Melbourne')
#------------- Twitter functions ------------------
def create_url(user_id):
    # Replace with user ID below
    return "https://api.twitter.com/2/users/{}/tweets".format(user_id)

def get_start_date(days_ago):
    '''time = datetime.now() gives you the local time whereas time = datetime.utcnow() 
    gives the local time in UTC. Hence now() may be ahead or behind which gives the  error'''
    time = datetime.utcnow()
    start_time = time - timedelta(days=days_ago)

    # Subtracting 15 seconds because api needs end_time must be a minimum of 10
    # seconds prior to the request time
    # end_time = time - timedelta(seconds=15)

    # convert to strings
    start_time = start_time.strftime(dtformat)
    return start_time

def get_end_date(days_ago):
    # time = datetime.now() gives you the local time whereas time = datetime.utcnow()
    # gives the local time in UTC. Hence now() may be ahead or behind which gives the
    # error

    time = datetime.utcnow()


    end_time = time-timedelta(days=days_ago)
    end_time = end_time.strftime(dtformat)
    return end_time

def get_params(start_time_days_ago=30, end_time_days_ago=None,max_results=10, pagination_token=None, since_id=None):
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    if pagination_token and pagination_token != 'init':
        if end_time_days_ago:
            params = {"tweet.fields": "author_id,created_at,in_reply_to_user_id,conversation_id","max_results":max_results,"pagination_token":pagination_token,"start_time":get_start_date(start_time_days_ago),"end_time":get_end_date(end_time_days_ago)}
        else:
            params = {"tweet.fields": "author_id,created_at,in_reply_to_user_id,conversation_id","max_results":max_results,"pagination_token":pagination_token,"start_time":get_start_date(start_time_days_ago)}
    else: 
        if end_time_days_ago:
            params = {"tweet.fields": "author_id,created_at,in_reply_to_user_id,conversation_id","max_results":max_results,"start_time":get_start_date(start_time_days_ago),"end_time":get_end_date(end_time_days_ago)}
        else:
            params = {"tweet.fields": "author_id,created_at,in_reply_to_user_id,conversation_id","max_results":max_results,"start_time":get_start_date(start_time_days_ago),"since_id":since_id}
    return params

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def get_query_meta_data(json_response):
    return json_response['meta']

def format_meta_data(json_response):
    df_meta_data_raw = pd.json_normalize(json_response['meta'])
    df_meta_data_raw['time_of_load'] = pd.Timestamp.now()

    df_meta_data = pd.concat([df_meta_data_columns,df_meta_data_raw])

    return df_meta_data

def format_raw_data(json_response,user_id):
    df = pd.json_normalize(json_response['data'])
    df_filtered = df 
    df_filtered['time_of_load'] = pd.Timestamp.now()
    
    #Force columns into data types.
    df_filtered['created_at'] = pd.to_datetime(df_filtered['created_at'])
    df_filtered['edit_history_tweet_ids'] = df_filtered['edit_history_tweet_ids'].astype(str)
    df_filtered['author_id'] = df_filtered['author_id'].astype(str)
    df_filtered['id'] = df_filtered['id'].astype(str)
    df_filtered['text'] = df_filtered['text'].astype(str)
    try:
        df_filtered['in_reply_to_user_id'] = df_filtered['in_reply_to_user_id'].astype(str)
    except KeyError:
        df_filtered['in_reply_to_user_id'] = None
    df_filtered['conversation_id'] = df_filtered['conversation_id'].astype(str)
    return df_filtered

def stage_data(raw_data):
    df_raw_data = raw_data
    df_data_extracted_info = df_raw_data.text.str.extract('(?P<external_tagging>\@[\w\s]+)?(?P<info_type>[\W]*)?(?P<train_lines>[\s\S]+lines?)\:(?P<info_on_issue>[\s\S]+)(?P<reason>due to[\s\S]+|while[\s\S]+|after[\s\S]+.olice[\s\S]+)\n\n',expand=True)
    df_stg_data = pd.concat([df_raw_data[['time_of_load','id','created_at','conversation_id','text']],df_data_extracted_info],axis=1)

    #Change Timezone
    #df_stg_data['created_at'] = df_stg_data.dt.tz_localize(timezone)

    #Split date, time, and hour into separate Columns 
    df_stg_data['created_date'] = df_stg_data.created_at.dt.date
    df_stg_data['created_time'] = df_stg_data.created_at.dt.time
    df_stg_data['created_hour'] = df_stg_data.created_at.dt.hour

    #Force columns into data format of choice
    #Force all strings
    column_types = {"external_tagging": "str",
                    "info_type": "str",
                    "train_lines":"str",
                    "info_on_issue": "str",
                    "reason":"str",
                    "created_hour":"int64"}
    for column,data_type in column_types.items():
        df_stg_data[column] = df_stg_data[column].astype(data_type)

    return df_stg_data

#------------- GCP functions ------------------

def get_client(key_path): 
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return client

def check_if_table_exists(client, table_id):
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False

def get_newest_tweet_id(client):
    if check_if_table_exists(client,table_id_meta_data):
        sql = """
        SELECT 
        MAX(newest_id) as newest_id
        FROM `gcp-project.analytics.meta_data_metro_trains_twitter` """

        df_latest_loaded_tweet = client.query(sql).to_dataframe()
        df_latest_loaded_tweet.head()
        latest_loaded_tweet = df_latest_loaded_tweet['newest_id'][0]
        return latest_loaded_tweet
    return None

def load_job(client,df,table_id,job_config):
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    return null 

def main_looper():
    pagination_token = 'init'
    while pagination_token:
        #Code below gets the latest tweet's id
        client = get_client(key_path)
        newest_id = get_newest_tweet_id(client)

        #The code below gets the tweets from Twitter
        url = create_url(user_id)
        
        params = get_params(start_time_days_ago=start_time_days_ago, end_time_days_ago=end_time_days_ago, max_results=max_results, pagination_token=pagination_token,since_id=newest_id)
        json_response = connect_to_endpoint(url, params)
        print(json.dumps(json_response, indent=4, sort_keys=True))
        
        #Check if the the meta data contains the next token. If not, set the pagination_token to None which will stop the while loop.
        if get_query_meta_data(json_response).get('next_token'):
            pagination_token = get_query_meta_data(json_response).get('next_token')
        else:
            pagination_token = None
        
        #The code below puts the raw meta data into a table.
        df_meta_data = format_meta_data(json_response)
        df_meta_data.head()

        #The code below does some basic cleaning and formatting for the data in prep for staging and loading.
        df_raw_data = format_raw_data(json_response,user_id)
        df_raw_data.head()
        
        # --- Code below transforms data into the staging data ---
        df_stg_data = stage_data(df_raw_data)
        df_stg_data.head()

        # --- Code below loads data to BigQuery ---

        #Load raw data
        job_meta_data = client.load_table_from_dataframe(df_meta_data, table_id_meta_data, job_config=job_config_meta_data) # Make an API request.
        job_meta_data.result()
        #Load raw data
        job_raw_data = client.load_table_from_dataframe(df_raw_data, table_id_raw_data, job_config=job_config_raw_data)  # Make an API request.
        job_raw_data.result()
        #Load staging data
        job_stg_data = client.load_table_from_dataframe(df_stg_data, table_id_stg_data, job_config=job_config_stg_data)  # Make an API request.
        job_stg_data.result()
        sleep(request_delay)
        
def hello_pubsub(event, context):
    main_looper()
