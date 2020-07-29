# importing libraries 
import psycopg2 
  
# a function to connect to 
# the database. 
def connect(): 
  
    # connecting to the database called postgres 
    # using the connect function 
    try: 
  
        conn = psycopg2.connect(database ="postgres",  
                            user = "postgres",  
                            password = "password",  
                            host = "localhost",  
                            port = "5432") 
  
        # creating the cursor object 
        cur = conn.cursor() 
      
    except (Exception, psycopg2.DatabaseError) as error: 
          
        print ("Error while creating PostgreSQL table", error) 
      
  
    # returing the conn and cur 
    # objects to be used later 
    return conn, cur 
  
  
# get the details frpm pg_stat_activity
def get_pg_stat_activity(): 
  
    conn, cur = connect() 
  
    # Number of Connection or running backend 
    try: 
        cur.execute('SELECT count(*) FROM pg_stat_activity; ') 
      #pg_stat_database_conflicts
    except: 
        print('error pg_stat_activity !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  CONNECTIONS ::::::::::::::::::::::::::::::::::")
    for row in numconn:
        #print("Number of Connection/ Running backend = ", row[0], )
        #print("<metric type=\"IntCounter\" name=\"Process| PG: Zombie:Total Number of Zombie Processes (#)\" value=\""+str(row[0])+"\" />" )
        print("<metric type=\"IntCounter\" name=\"Process| PG: Number of Connections / Running backend (#)\" value=\""+str(row[0])+"\" />" )


    # Number of backend waiting on locks
    lcks='Lock'
    try: 
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event = %s", (lcks,) );
    except: 
        print('error pg_stat_activity backend on locks !') 
  
    # store the result in data 
    numconnlocks = cur.fetchall() 
    for row in numconnlocks:
        #print("Backend waiting on locks = ", row[0], )   
        print("<metric type=\"IntCounter\" name=\"Process| PG: Number of Backend waiting on locks (#)\" value=\""+str(row[0])+"\" />" ) 

    # Number of backend Idel in Transactions
    idls='idle in transaction'
    try: 
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event = %s", (idls,) );
    except: 
        print('error pg_stat_activity backend idle !') 
  
    # store the result in data 
    numconnidle = cur.fetchall() 
    for row in numconnidle:
        #print("Backend idle in transactions = ", row[0], )  
        print("<metric type=\"IntCounter\" name=\"Process| PG: Number of Backend idle in transactions (#)\" value=\""+str(row[0])+"\" />" ) 

    #session holding or awaiting each lock                    
    try: 
        cur.execute('SELECT * FROM pg_locks pl LEFT JOIN pg_stat_activity psa ON pl.pid = psa.pid;')
    except: 
        print('error session holding or awaiting each lock !') 
  
    # store the result in data 

    # sessionhold = cur.fetchall() 
    # print("::::::::::::::::::::::::::::::  SESSION HOLD & LOCK ::::::::::::::::::::::::::::::::::")
    # print(sessionhold)
    # for row in sessionhold:
    #     print(sessionhold)

    # Top Function call 

    try: 
        cur.execute('SELECT datname, usename, backend_xid FROM pg_stat_activity WHERE backend_xid IS NOT NULL;' )
      #pg_stat_database_conflicts
    except: 
        print('error backend_xid from pg_stat_activity !') 
  
    # store the result in data 
    xid = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  TRANSACTION IDENTIFIER ::::::::::::::::::::::::::::::::::")
    for row in xid:
        print("***Database**** "+str(row[0])+ " ***Usr**** "+ str(row[1]))
        print("Top-level transaction identifier of this backend = ", row[2], )


    if conn is not None:
        conn.close()
        #print('Database connection closed.')
    # return the result 
    #return data 

# driver function 
if __name__ == '__main__': 
  
    #pg_stat_activity 
    get_pg_stat_activity()

