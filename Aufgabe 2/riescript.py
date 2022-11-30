import requests
import hidden
import psycopg2
import json

year = 2022
month = 11
r = requests.get("https://api.nytimes.com/svc/archive/v1/"+str(year)+ "/"+ str(month) +".json?api-key=TH8tUlGkry6XCADNwAZ4VSTEmkJ7YqQH") 
#get the count of articles
count = len(r.json()['response']['docs'])
print("The count of articles in "+str(year)+"/"+str(month)+" is "+str(count))