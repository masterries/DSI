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


#execute sql query on table "articlesries" and return result as table

def executeSQL(sql):
    conn = psycopg2.connect(hidden.psycopg2(hidden.secrets()))
    cur = conn.cursor()
    cur.execute(sql)
    t = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return t

#get NYTimes most viewed articles with period (1, 7, 30)
def getNYTMostViewed(api = hidden.getApi(), period = 30, param1 = "viewed"):
    if period not in [1, 7, 30]:
        print("Period must be 1, 7 or 30")
        return None
    url = "https://api.nytimes.com/svc/mostpopular/v2/"+ str(param1) +"/" + str(period) + ".json?api-key=" + api
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()['results']
#insert NYTimes most viewed articles with period (1, 7, 30) into table "mostviewedRies"
def insertNYTMostViewed(api = hidden.getApi(), period = 30, table = "mostviewedRies", force = False , param1 = "viewed"):
    checktable = createTable(table, force)
    if(not checktable):
        print("No Data inserted (use force = True to overwrite table)")
        return False

    r = getNYTMostViewed(api, period, param1)
    print(param1)
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


def getNYTBestsellerList(api = hidden.getApi(), date = "2021-01-01"):
    url = "https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date=" + date + "&api-key=" + api

    r = requests.get(url)
    if r.status_code == 200:
        return r.json()['results']["lists"]["books"]

#iterate getNYTBestsellerList for every day in a month and insert into table "bestsellerRies"
def insertNYTBestsellerList(api = hidden.getApi(), month = 11, year = 2022, table = "ds22m032_bestsellerries", force = False):
    
    conn = psycopg2.connect(hidden.psycopg2(hidden.secrets()))
    cur = conn.cursor()

    for i in range(1, 31):
        if i < 10:
            date = str(year) + "-" + str(month) + "-0" + str(i)
        else:
            date = str(year) + "-" + str(month) + "-" + str(i)
        r = getNYTBestsellerList(api, date)
        if r is not None:
            cur.executemany("INSERT INTO " + table + " VALUES (%s);", [(json.dumps(article),) for article in r])
            conn.commit()
            print("Data inserted for " + date)
    cur.close()
    conn.close()
    return True





#get https://api.nytimes.com/svc/books/v3/lists/names.json
def getNYTBestsellerListNames(api = hidden.getApi()):
    url = "https://api.nytimes.com/svc/books/v3/lists/names.json?api-key=" + api
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()['results']
    
#insert https://api.nytimes.com/svc/books/v3/lists/names.json into table "ds22m032_bestsellerRies"
def insertNYTBestsellerListNames(api = hidden.getApi(), table = "ds22m032_bestsellerries"):

    r = getNYTBestsellerListNames(api)
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

insertNYTBestsellerListNames()




