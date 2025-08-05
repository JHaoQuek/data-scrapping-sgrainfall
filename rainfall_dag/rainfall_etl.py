import json 
import requests
import pandas as pd 
import boto3 # for S3
import io # to create parquet in Memory (instead of Disk)
from datetime import datetime, timedelta
import psycopg2 # for Redshift
import googlemaps
import time

def google_map_area(location):

    ### Google map to get area info 
    gmaps = googlemaps.Client(key='')

    latitude = location['latitude']
    longitude = location['longitude']

    result = gmaps.reverse_geocode((latitude, longitude))
    area_name = None
    if result:
        for every_result in result:
            for component in every_result['address_components']:
                if 'neighborhood' in component['types']:
                    area_name = component['long_name']
                    return area_name
    
def run_rainfall_etl():

    formatted_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "date": formatted_time ## "2025-06-06T12:27:24" ## datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    }
    resp = requests.get("https://api-open.data.gov.sg/v2/real-time/api/rainfall", params=params)
    data = resp.json() ## throw error if not JSON file ## turn into a Python dict / object 

    if resp.status_code == 200:

        ### store stations data (object) into dataframe 
        stations = []
        for ele in data['data']['stations']:

            refined_data = {
                'station_id': ele['id'], 
                # 'device_id': ele['device_id'], 
                'station_name': ele['name'], 
                'location': ele['location']
            }
            stations.append(refined_data)
        stations_df = pd.DataFrame(stations)


        ## get timestamp data from API data 
        timestamp = data['data']['readings'][0]['timestamp']


        # store rainfall data (object) into dataframe 
        rainfall = []
        for ele in data['data']['readings'][0]['data']:
            refined_data2 = {
                'station_id': ele['stationId'],
                'rainfall': ele['value']
            }
            rainfall.append(refined_data2)
        rainfall_df = pd.DataFrame(rainfall)


        ### perform joining of 2 dataframe by station_id column
        df = pd.merge(stations_df, rainfall_df, how='outer', on='station_id')
        df['timestamp'] = timestamp
        df['date'] = timestamp[:10]


        ### Uses google map API to get area info based on longitude and latitude 
        df['area_name'] = df['location'].apply(google_map_area)


        ### change datatype to ensure redshift declare the correct datatype for these columns 
        df = df.astype({
            "station_id": "string", 
            "station_name": "string", 
            "location": "string", 
            "rainfall": "double", 
            "timestamp": "string",
            "date": "string",
            "area_name": "string"
        })


        ### Import data to S3 
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow')
        s3 = boto3.client('s3')
        bucket_name = 'project1-rainfall-raw-ap-southeast-1-dev'
        s3_key = 'rainfall_data/' + timestamp[:10] + '/' + timestamp[10:19] + ".parquet"
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket_name, s3_key)


        ### connect Redshift
        conn = psycopg2.connect(
            dbname='rainfall-db',
            host='project1-rainfall-workgroup.013611968097.ap-southeast-1.redshift-serverless.amazonaws.com',
            port='5439',
            user='admin',
            password='jiahaoQuek123',
            sslmode='require'
        )
        cur = conn.cursor()


        ### delete existing data of this particular date - prevent duplication in Redshift
        delete_sql = """
            DELETE FROM rainfall_data 
            WHERE timestamp = %s
        """
        cur.execute(delete_sql, (timestamp, ))  
        conn.commit()


        ### Run copy command to load data from S3 into Redshift
        copy_sql = f"""
            COPY rainfall_data
            FROM 's3://{bucket_name}/{s3_key}'
            IAM_ROLE 'arn:aws:iam::013611968097:role/service-role/AmazonRedshift-CommandsAccessRole-20250608T174503'
            PARQUET
            REGION 'ap-southeast-1';
        """

        cur.execute(copy_sql)
        conn.commit()

        cur.close()
        conn.close()

    else:
        print("Error:", resp.status_code, resp.json().get("errorMsg"))
    