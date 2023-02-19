import sqlite3

conn = sqlite3.connect("C:\\Users\\lespadah\\Desktop\\Proyectos\\Spotify_ETL\\my_played_tracks.sqlite")
cursor = conn.cursor()

sql_query ="""SELECT * FROM my_played_tracks;"""

cursor.execute(sql_query)


result_set = cursor.fetchall()


for reg in result_set :
    print(reg)

    


conn.close()