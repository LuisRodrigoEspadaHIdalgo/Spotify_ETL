import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3
from decouple import config

#url gor get token
#https://developer.spotify.com/console/get-recently-played/

DATABASE_LOCATION = config('DATABASE_LOCATION')
USER_ID = config('USER_ID')
TOKEN = config('TOKEN')


#Function Validation
def check_if_valid_data(df: pd.DataFrame) -> bool:
    #Chek id dataframe is empty
    if df.empty:
        print("No songs donwloaded. Finishing execution")
        return False

    #Primary Key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary hey check is violated")
    
    #Check for nulls:
    if df.isnull().values.any():
        raise Exception("Null values found")

    #Check that all timestamps are of yesterday's date
    yesterday = datetime.now() - timedelta(days = 60)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    
    # for timestamp in timestamps:
    #     if datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #         raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    
    return True 



if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.now()
    yesterday = today - timedelta(days = 3)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers = headers)

    data = r.json()


    #print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []


    for song in data['items']:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict,columns = ["song_name","artist_name","played_at","timestamp"])

    #Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")


    #Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect(r"C:\Users\lespadah\Desktop\Proyectos\Spotify_ETL\my_played_tracks.sqlite",uri=True)
    cursor = conn.cursor()

    sql_query = """
                CREATE TABLE IF NOT EXISTS my_played_tacks(
                    song_name VARCHAR(200),
                    artist_name VARCHAR(200),
                    played_at  VARCHAR(200) NOT NULL PRIMARY KEY,
                    timestamp VARCHAR(200)
                );
    """
    
    cursor.execute(sql_query)
    print("open database secessfully")

    try :
        #song_df.to_sql("my_played_tracks", conn, index = False, if_exists ='append')
        #song_df.to_sql("my_played_tracks", engine, index = False, if_exists ='append')
        for i in range(len(song_df)):
            var_song_name = song_df.iloc[i]['song_name']
            var_artist_name = song_df.iloc[i]['artist_name']
            var_played_at = song_df.iloc[i]['played_at']
            var_timestamp = song_df.iloc[i]['timestamp']

            print(var_song_name)
            print(var_artist_name)
            print(var_played_at)
            print(var_timestamp)

            sql_query2 = """
                INSERT OR IGNORE INTO my_played_tacks(song_name,artist_name,played_at,timestamp) 
                                            VALUES (?,?,?,?);
                                            
            """
            tupla = (var_song_name,var_artist_name,var_played_at,var_timestamp)
            print(f"tupla:{tupla}")
            print(len(tupla))
            cursor.execute(sql_query2,[var_song_name,var_artist_name,var_played_at,var_timestamp])
            conn.commit()
            
    except  Exception as e:
        print(e)
        print("Data already exist in the database")
    
    
    
    conn.close()
    print("Close database successfully")

    #print(song_df)