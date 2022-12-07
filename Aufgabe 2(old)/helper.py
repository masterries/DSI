import requests
import hidden
import psycopg2
import json

baseURL = "https://api.nytimes.com/svc/archive/v1/"
#check if table "articlesries" exists
def checkTable(table = "articlesries"):
    conn = psycopg2.connect(hidden.psycopg2(hidden.secrets()))
    cur = conn.cursor()
    #check if table exists
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (table,))
    tableExists = cur.fetchone()[0]
    conn.close()
    return tableExists

#get Request from API
def getNYCMonthRequest(month = 11, year = 2022, api = hidden.getApi()):
    return requests.get(baseURL + str(year) + "/" + str(month) + ".json?api-key=" + api)

#create table JSONB
def createTable(table = "articlesries" , force = False):
    #if table already exists, return delete table
    if(checkTable(table) and not force):
        print( "Table already exists")
        return False
    elif(force):
        conn = psycopg2.connect(hidden.psycopg2(hidden.secrets()))
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS " + table + ";")
        cur.execute("CREATE TABLE " + table + " (body JSONB);")
        conn.commit()
        cur.close()
        conn.close()
        print ("Table "+ table + " created")
        return True


#insert data into table "articlesries"
def insertNYCArticles(month = 11, year = 2022, api = hidden.getApi(), table = "articlesries", force = False):
    checktable = createTable(table, force)
    if(not checktable):
        print("No Data inserted (use force = True to overwrite table)")
        return False



    r = getNYCMonthRequest(month, year, api)
    if r.status_code == 200:
        conn = psycopg2.connect(hidden.psycopg2(hidden.secrets()))
        cur = conn.cursor()
        #insert articles r.json()['response']['docs'] into table with execute many
        cur.executemany("INSERT INTO " + table + " VALUES (%s);", [(json.dumps(article),) for article in r.json()['response']['docs']])
        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted")
        return True


        #for i in range(len(r.json()['response']['docs'])):
        #    articles = r.json()['response']['docs'][i]
        #    cur.execute("INSERT INTO " + table + " (body) VALUES (%s);", (json.dumps(articles),))

        #    conn.commit()
        #   print("Inserted article " + str(i))


#get NYTimes most viewed articles with period (1, 7, 30)
def getNYTMostViewed(api = hidden.getApi(), period = 30):
    if period not in [1, 7, 30]:
        print("Period must be 1, 7 or 30")
        return None

    r = requests.get("https://api.nytimes.com/svc/mostpopular/v2/viewed/" + str(period) + ".json?api-key=" + api)
    if r.status_code == 200:
        return r.json()['results']
#insert NYTimes most viewed articles with period (1, 7, 30) into table "mostviewedRies"
def insertNYTMostViewed(api = hidden.getApi(), period = 30, table = "mostviewedRies", force = False):
    checktable = createTable(table, force)
    if(not checktable):
        print("No Data inserted (use force = True to overwrite table)")
        return False

    r = getNYTMostViewed(api, period)
    if r is not None:
        conn = psycopg2.connect(hidden.psycopg2(hidden.secrets()))
        cur = conn.cursor()
        #insert articles r.json()['response']['docs'] into table with execute many
        cur.executemany("INSERT INTO " + table + " VALUES (%s);", [(json.dumps(article),) for article in r])
        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted")
        return True




