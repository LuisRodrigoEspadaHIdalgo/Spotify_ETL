import json
import requests
 
# Making a get request
response = requests.get('https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=')
# print response
print(response)

print("-------------------------------------------------------------------------------------------------------")
print("                                                   ")

# print json content
print(response.json())