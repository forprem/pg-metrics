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
    print("::::::::::::::::::::::::::::::  CONNECTIONS ::::::::::::::::::::::::::::::::::")
    for row in numconn:
        print("Number of Connection/ Running backend = ", row[0], )

    # Number of backend waiting on locks
    lcks='Lock'
    try: 
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event = %s", (lcks,) );
    except: 
        print('error pg_stat_activity backend on locks !') 
  
    # store the result in data 
    numconnlocks = cur.fetchall() 
    for row in numconnlocks:
        print("Backend waiting on locks = ", row[0], )    

    # Number of backend Idel in Transactions
    idls='idle in transaction'
    try: 
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event = %s", (idls,) );
    except: 
        print('error pg_stat_activity backend idle !') 
  
    # store the result in data 
    numconnidle = cur.fetchall() 
    for row in numconnidle:
        print("Backend idle in transactions = ", row[0], )  

    #session holding or awaiting each lock                    
    try: 
        cur.execute('SELECT * FROM pg_locks pl LEFT JOIN pg_stat_activity psa ON pl.pid = psa.pid;')
    except: 
        print('error session holding or awaiting each lock !') 
  
    # store the result in data 

    sessionhold = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  SESSION HOLD & LOCK ::::::::::::::::::::::::::::::::::")
    print(sessionhold)
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
    print("::::::::::::::::::::::::::::::  TRANSACTION IDENTIFIER ::::::::::::::::::::::::::::::::::")
    for row in xid:
        print("***Database**** "+str(row[0])+ " ***Usr**** "+ str(row[1]))
        print("Top-level transaction identifier of this backend = ", row[2], )


# get the details frpm pg_stat_database_conflicts
def get_pg_stat_database_conflicts(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    try: 
        cur.execute('SELECT count(*) FROM pg_stat_database_conflicts; ') 
      #get_pg_stat_user_functions()
    except: 
        print('error !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    for row in numconn:
        print("Number of Database Conflicts = ", row[0], )



# get the details frpm pg_stat_database
def get_pg_stat_database(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    db_name='awx'
    try: 
        cur.execute("SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks, datname, temp_bytes FROM pg_stat_database where datname= %s", (db_name,) ); 
      #
    except: 
        print('error get_pg_stat_database !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  TRANSACTION DATA ::::::::::::::::::::::::::::::::::")

    for row in numconn:
        print("************* Database ******************** "+str(row[5]))
        print("Number of transactions have been Committed = "+str(row[0]))
        print("Number of transactions have been Rollbacked = "+str(row[1]))
        print("Number of disk blocks read in this Database = "+str(row[2]))
        print("Number of times disk blocks were found already in the buffer cache = "+str(row[3]))
        print("Number of Deadlocks = "+str(row[4])) 
        print("Total amount of data written to temporary files by queries in this database = "+str(row[6])) 

    #AWX DB Size
    try: 
        cur.execute("SELECT pg_database.datname, pg_database_size(pg_database.datname) as size_bytes FROM pg_database where datname= %s", (db_name,) ); 
      #
    except: 
        print('error get_pg_stat_database !') 
  
    # store the result in data 
    dbsize = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  AWX DB SIZE ::::::::::::::::::::::::::::::::::")

    for row in dbsize:
        print("************* Database ******************** "+str(row[0]))    
        print("************* AWX DB SIZE IN BYTES ******************** "+str(row[1]))     
          
def get_pg_stat_user_functions(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    try: 
        cur.execute('SELECT count(*) FROM pg_stat_user_functions; ') 
      #
    except: 
        print('error !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    for row in numconn:
        print("Number of Sessions = ", row[0], )

  
# get the details frpm pg_stat_database
def get_pg_stat_all_tables(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    dead_rows=50
    try: 
        cur.execute("SELECT vacuum_count, autovacuum_count, analyze_count, autoanalyze_count, last_vacuum, last_autovacuum, schemaname, relname, n_dead_tup, n_live_tup, idx_scan  FROM pg_stat_all_tables where n_dead_tup > %s", (dead_rows,) ); 
        
    except: 
        print('error get_pg_stat_database !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  VACUUM INFO ::::::::::::::::::::::::::::::::::")
    for row in numconn:
        print("***Schema**** "+str(row[6])+ " ***Table**** "+ str(row[7]))
        print("Number of times this table has been manually vacuumed = "+str(row[0]))
        print("Number of times this table has been vacuumed by the autovacuum daemon = "+str(row[1]))
        print("Number of times this table has been manually analyzed = "+str(row[2]))
        print("Number of times this table has been analyzed by the autovacuum daemon = "+str(row[3]))
        print("Last time at which this table was manually vacuumed  = "+str(row[4])) 
        print("Last time at which this table was vacuumed by the autovacuum daemon  = "+str(row[5])) 
        print("Dead Rows = "+str(row[8]))
        print("Tables with most live rows = "+str(row[9]))
        print("Most frequent scanned index = "+str(row[10]))
        print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")

    #
    zero = 0
    try: 
        cur.execute("SELECT schemaname, relname, n_live_tup, n_dead_tup, last_autovacuum FROM pg_stat_all_tables where n_live_tup > %s", (zero,) ); 
      #
    except: 
        print('error LIVE & DEAD TUPLES!') 
  
    # store the result in data 
    print("::::::::::::::::::::::::::::::  LIVE & DEAD TUPLES ::::::::::::::::::::::::::::::::::")
    livedead = cur.fetchall() 
    for row in livedead:
        print("***Schema**** "+str(row[0])+ " ***Table**** "+ str(row[1]))
        print("Live tuples = ", row[2], )
        print("Dead tuples = ", row[3], )
        print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")


# get the details frpm pg_stat_bgwriter
def get_pg_stat_bgwriter(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    try: 
        cur.execute('SELECT buffers_alloc, buffers_checkpoint, buffers_clean, buffers_backend FROM pg_stat_bgwriter;' )
        
      #
    except: 
        print('error pg_stat_bgwriter !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  BUFFER ::::::::::::::::::::::::::::::::::")
    for row in numconn:
        print("***CHECKPOINTS**** buffers allocated  "+str(row[0]))
        print("Number of buffers written during checkpoints = "+str(row[1]))
        print("Number of buffers written by the background writer = "+str(row[2]))
        print("Number of buffers written directly by a backend  = "+str(row[3])) 

    #WAL Archive Count
    # stts = 'pg_xlog/archive_status'
    # rdy = '^[0-9A-F]{24}.ready$'
    # try: 
    #     cur.execute("SELECT count(*) FROM pg_ls_dir(%s) WHERE pg_ls_dir ~ %s", (stts, rdy,) ); 
    # except: 
    #     print('error WAL Archive Count !') 
    # # store the result in data 
    # walcnt = cur.fetchall() 
    # print("::::::::::::::::::::::::::::::  WAL Archive Count ::::::::::::::::::::::::::::::::::")
    # for row in walcnt:
    #     print("***WAL Archive Count *****  "+str(row[0]))



# get the details frpm pg_stat_user_tables
def get_pg_stat_user_tables(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from pg_stat_user_tables 
    dt_format='1970-01-01Z'
    try: 
        cur.execute("SELECT current_database(), schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, COALESCE(last_vacuum, %s) as last_vacuum, COALESCE(last_autovacuum, %s) as last_autovacuum, COALESCE(last_analyze, %s) as last_analyze, COALESCE(last_autoanalyze, %s) as last_autoanalyze, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count FROM pg_stat_user_tables where seq_scan > 25 order by seq_tup_read desc limit 5;", (dt_format,dt_format,dt_format,dt_format,) );
      #
    except: 
        print('error pg_stat_user_tables !') 
  
    # store the result in data 
    clientinfo = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  USER STATS ::::::::::::::::::::::::::::::::::")
    #print(clientinfo)
    for row in clientinfo:
        print("Database: "+str(row[0])+ "***Schema**** "+str(row[1])+ " ***Table**** "+ str(row[2]))
        print("Number of sequential scans initiated on this table = ", row[3], ) #seq_scan
        print("Number of live rows fetched by sequential scans = ", row[4], )    #seq_tup_read
        print("Number of index scans initiated on this table = ", row[5], )      #idx_scan
        print("Number of live rows fetched by index scans = ", row[6], )         #idx_tup_fetch
        print("Number of rows inserted = ", row[7], )                            #n_tup_ins
        print("Number of rows updated = ", row[8], )                             #n_tup_upd
        print("Number of rows deleted = ", row[9], )                             #n_tup_del
        print("Number of rows HOT updated  = ", row[10], )                       #n_tup_hot_upd
        print("Estimated number of live rows  = ", row[11], )                    #n_live_tup
        print("Estimated number of dead rows  = ", row[12], )                    #n_dead_tup
        print("Estimated number of rows modified since this table was last analyzed  = ", row[13], ) #n_mod_since_analyze
        print("Last time at which this table was manually vacuumed   = ", row[14], )                 #last_vacuum
        print("Last time at which this table was vacuumed by the autovacuum daemon  = ", row[15], )  #last_autovacuum
        print("Last time at which this table was manually analyzed  = ", row[16], )                  #last_analyze  
        print("Last time at which this table was analyzed by the autovacuum daemon   = ", row[17], ) #last_autoanalyze
        print("Number of times this table has been manually vacuumed  = ", row[18], )                #vacuum_count
        print("Number of times this table has been vacuumed by the autovacuum daemon  = ", row[19], ) #autovacuum_count
        print("Number of times this table has been manually analyzed  = ", row[20], )                 #analyze_count
        print("Number of times this table has been analyzed by the autovacuum daemon  = ", row[21], ) #autoanalyze_count
        print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")


# get the details frpm pg_stat_replication
def get_pg_stat_replication(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from get_pg_stat_replication 
    #dt_format='1970-01-01Z'
    try: 
        cur.execute("SELECT application_name, client_hostname, backend_start, state, write_location  FROM pg_stat_replication;"  )
      #cur.execute("SELECT current_database(), schemaname, relname FROM get_pg_stat_replication where seq_scan > 25 order by seq_tup_read desc limit 5;", (dt_format,dt_format,dt_format,dt_format,dt_format,) );
    except: 
        print('error get_pg_stat_replication !') 
  
    # store the result in data 
    replicationinfo = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  REPLICATION CLIENT INFO ::::::::::::::::::::::::::::::::::")
    for row in replicationinfo:
        print("***REPLICATIONS**** Application Name  "+str(row[0]))
        print("Client HostName = "+str(row[1]))
        print("Time client connected to this WAL sender = "+str(row[2]))
        print("WAL sender state  = "+str(row[3])) 
        print("Last transaction log position written to disk by this standby server = "+str(row[4]))
        print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")

    
    # LAG Information 
    try:
        cur.execute("SELECT write_location - sent_location AS write_lag, flush_location - write_location AS flush_lag, replay_location - flush_location AS replay_lag FROM pg_stat_replication;")
    except:
        print("error get_pg_stat_replication 2 !")

    replicationinfo2 = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  REPLICATION LAG ::::::::::::::::::::::::::::::::::")  
    for row in replicationinfo:
        print("****** Write Lag ******: "+str(row[0])) 
        print("******* Flush Lag ******: "+str(row[1])) 
        print("****** Replay Lag ******: "+str(row[2])) 
        print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")

    # LAG In Bytes pg_replication_slots;
    try:
        #cur.execute("SELECT restart_lsn() - confirmed_flush_lsn FROM pg_replication_slots;")
        cur.execute("SELECT * FROM pg_replication_slots;")
    except:
        print("error pg_replication_slots; !")

    replicationslot = cur.fetchall()   
    for row in replicationslot:
        print(replicationslot) 

    # Inactive replication Slots
    try: 
        cur.execute('SELECT count(*) FROM pg_replication_slots WHERE NOT active; ') 
      #
    except: 
        print('error Inactive replication slots !') 
  
    # store the result in data 
    inreplslots = cur.fetchall() 
    for row in inreplslots:
        print("Inactive replication slots = ", row[0], )


def get_pg_statio_all_tables(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    try: 
        cur.execute('SELECT current_database(), schemaname, relname, heap_blks_read FROM pg_statio_all_tables order by heap_blks_read desc fetch first 5 rows only; ') 
      #
    except: 
        print('error get_pg_statio_all_tables !') 
  
    # store the result in data 
    mostdiskusage = cur.fetchall() 
    print("::::::::::::::::::::::::::::::  DISK USAGE ::::::::::::::::::::::::::::::::::")
    for row in mostdiskusage:
        print("Database: "+str(row[0])+ "***Schema**** "+str(row[1])+ " ***Table**** "+ str(row[2]))
        print("Table with most DISK Usage = ", row[3], )     


    if conn is not None:
        conn.close()
        print('Database connection closed.')
    # return the result 
    #return data 

# driver function 
if __name__ == '__main__': 
  
    #pg_stat_activity 
    get_pg_stat_activity()

    #pg_stat_database_conflicts
    get_pg_stat_database_conflicts()

    #pg_stat_database 
    get_pg_stat_database()

    #pg_stat_user_functions
    get_pg_stat_user_functions()

    #pg_stat_all_tables
    get_pg_stat_all_tables()

    #pg_stat_bgwriter 
    get_pg_stat_bgwriter()

    #pg_stat_user_tables
    get_pg_stat_user_tables()

    #pg_stat_replication (Replication)
    get_pg_stat_replication()

    #pg_statio_all_tables 
    get_pg_statio_all_tables()

