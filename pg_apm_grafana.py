# importing libraries 
import psycopg2 
import math
  
# a function to connect to 
# the database. 
def connect(): 
  
    # connecting to the database called awx 
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
        #print("<metric type=\"IntCounter\" name=\"Postgres| PG: Zombie:Total Number of Zombie Postgreses (#)\" value=\""+str(row[0])+"\" />" )
        print("<metric type=\"IntCounter\" name=\"Postgres| Activity: Number of Connections / Running backend (#)\" value=\""+str(row[0])+"\" />" )


    # Number of backend waiting on locks
    lcks='Lock'
    try: 
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = %s", (lcks,) );
    except: 
        print('error pg_stat_activity backend on locks !') 
  
    # store the result in data 
    numconnlocks = cur.fetchall() 
    for row in numconnlocks:
        #print("Backend waiting on locks = ", row[0], )   
        print("<metric type=\"IntCounter\" name=\"Postgres| Activity: Number of Backend waiting on locks (#)\" value=\""+str(row[0])+"\" />" ) 

    # Number of backend Idel in Transactions
    idls='idle in transaction'
    try: 
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = %s", (idls,) );
    except: 
        print('error pg_stat_activity backend idle !') 
  
    # store the result in data 
    numconnidle = cur.fetchall() 
    for row in numconnidle:
        #print("Backend idle in transactions = ", row[0], )  
        print("<metric type=\"IntCounter\" name=\"Postgres| Activity: Number of Backend idle in transactions (#)\" value=\""+str(row[0])+"\" />" ) 

    #session holding or awaiting each lock                    
    try: 
        cur.execute('SELECT count(*) FROM pg_locks pl LEFT JOIN pg_stat_activity psa ON pl.pid = psa.pid where pl.mode = \'ExclusiveLock\' ;')
    except: 
        print('error session holding or awaiting each lock !') 
  
    # store the result in data 

    sessionhold = cur.fetchall() 
    # print("::::::::::::::::::::::::::::::  SESSION HOLD & LOCK ::::::::::::::::::::::::::::::::::")
    # print(sessionhold)
    for row in sessionhold:
        print("<metric type=\"IntCounter\" name=\"Postgres| Activity: Number of Query with ExclusiveLock (#)\" value=\""+str(row[0])+"\" />" ) 

    # Top Function call 
    # try: 
    #     cur.execute('SELECT datname, usename, backend_xid FROM pg_stat_activity WHERE backend_xid IS NOT NULL;' )
    #   #pg_stat_database_conflicts
    # except: 
    #     print('error backend_xid from pg_stat_activity !') 
  
    # # store the result in data 
    # xid = cur.fetchall() 
    # #print("::::::::::::::::::::::::::::::  TRANSACTION IDENTIFIER ::::::::::::::::::::::::::::::::::")
    # for row in xid:
    #     print("***Database**** "+str(row[0])+ " ***Usr**** "+ str(row[1]))
    #     print("Top-level transaction identifier of this backend = ", row[2], )

# get the details frpm pg_stat_database_conflicts
def get_pg_stat_database_conflicts(): 
    #Enable if above there is no connection
    # Grafana pg_stat_database_conflicts{instance="$instance", datname=~"$datname",server="$database_server"}
    conn, cur = connect() 
  
    # select all the rows from emp 
    try: 
        cur.execute('SELECT count(*) FROM pg_stat_database_conflicts; ') 
      #get_pg_stat_user_functions()
    except: 
        print('error pg_stat_database_conflicts!') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    for row in numconn:
        print("<metric type=\"IntCounter\" name=\"Postgres| DBConflicts: Number of Database Conflicts in Server (#)\" value=\""+str(row[0])+"\" />" ) 

# get the details frpm pg_stat_database
def get_pg_stat_database(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    db_name='awx'
    # QPS in Grafana eg. pg_stat_database_xact_commit{datname=~"$datname",instance=~"$instance", server=~"$database_server"
    # pg_settings_effective_cache_size_bytes{instance="$instance",server="$database_server"}
    # pg_stat_database_blks_hit{instance="$instance", datname=~"$datname", server="$database_server"} / (pg_stat_database_blks_read{instance="$instance", datname=~"$datname", server="$database_server"} + pg_stat_database_blks_hit{instance="$instance", datname=~"$datname", server="$database_server"})
    # pg_stat_database_temp_bytes{instance="$instance", datname=~"$datname",server="$database_server"}

    #blks_read, blks_hit: Number of times disk blocks were read vs. number of cache hits for these blocks.
    try: 
        cur.execute("SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks, datname, temp_bytes FROM pg_stat_database where datname= %s", (db_name,) ); 
      #
    except: 
        print('error get_pg_stat_database !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  TRANSACTION DATA ::::::::::::::::::::::::::::::::::")
 
    for row in numconn:
        #print("************* Database ******************** "+str(row[5]))
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Number of transactions have been Committed (#)\" value=\""+str(row[0])+"\" />" ) 
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Number of transactions have been Rollbacked (#)\" value=\""+str(row[1])+"\" />" ) 
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Number of disk blocks read in this Database (#)\" value=\""+str(row[2])+"\" />" ) 
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Number of Disk blocks found in the buffer cache (#)\" value=\""+str(row[3])+"\" />" ) 
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Number of Deadlocks (#)\" value=\""+str(row[4])+"\" />" ) 
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Data written to temporary files by queries in DB (#)\" value=\""+str(row[6])+"\" />" ) 

    #AWX DB Size
    # Grafana pg_postmaster_start_time_seconds{instance=~"$instance",server="$database_server"}
    try: 
        cur.execute("SELECT pg_database.datname, pg_database_size(pg_database.datname) as size_bytes FROM pg_database where datname= %s", (db_name,) ); 
      #
    except: 
        print('error get_pg_stat_database !') 
  
    # store the result in data 
    dbsize = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  AWX DB SIZE ::::::::::::::::::::::::::::::::::")

    for row in dbsize:
        #print("************* Database ******************** "+str(row[0])) 
        db_size = human_size(row[1])
        data = db_size.split()
        print("<metric type=\"IntCounter\" name=\"Postgres| DB_STAT: Awx db size in "+data[1]+" (#)\" value=\""+str((int(float(data[0]))))+"\" />" )    
    
          
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
        #print("Number of Sessions = ", row[0], )
        print("<metric type=\"IntCounter\" name=\"Postgres| USER: Total Number of Tracked Function Sessions (#)\" value=\""+str(row[0])+"\" />" )    


# get the details frpm pg_stat_database
def get_pg_stat_all_tables(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    #Grafana  pg_autovacuum_table_stat_n_dead_tup{instance="$instance",schemaname="public",server=~"$database_server"}
    #
    dead_rows=0
    try: 
        cur.execute('SELECT vacuum_count, autovacuum_count, analyze_count, autoanalyze_count, last_vacuum, last_autovacuum, schemaname, relname, n_dead_tup, n_live_tup, idx_scan  FROM pg_stat_all_tables ORDER BY n_dead_tup DESC LIMIT 1;') 
        #cur.execute('SELECT vacuum_count, autovacuum_count, analyze_count, autoanalyze_count, last_vacuum, last_autovacuum, schemaname, relname, n_dead_tup, n_live_tup, idx_scan  FROM pg_stat_all_tables;'  )
    except: 
        print('error get_pg_stat_all_tables !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  VACUUM INFO ::::::::::::::::::::::::::::::::::")
    for row in numconn:
        #print(row)
        #print("***Schema**** "+str(row[6])+ " ***Table**** "+ str(row[7]))
        last_tm_vacuum = row[4]
        last_tm_autovacuum = row[5]
        schema = row[6]
        table = row[7]
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Table manually vacuumed Relation Name "+table+" (#)\" value=\""+str(row[0])+"\" />" )    
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Table vacuumed by the autovacuum daemon Relation Name "+table+" (#)\" value=\""+str(row[1])+"\" />" )    
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Table manually analyzed Relation Name "+table+"  (#)\" value=\""+str(row[2])+"\" />" )    
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Table analyzed by the autovacuum daemon Relation Name "+table+" (#)\" value=\""+str(row[3])+"\" />" )    
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Most Dead Rows  Relation Name "+table+" (#)\" value=\""+str(row[8])+"\" />" )    
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Most live rows Relation Name "+table+" (#)\" value=\""+str(row[9])+"\" />" )    
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Most scanned index Relation Name "+table+" (#)\" value=\""+str(row[10])+"\" />" )    

        #print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")

    #
    zero = 0
    try: 
        cur.execute("SELECT schemaname, relname, n_live_tup, n_dead_tup, last_autovacuum FROM pg_stat_all_tables where n_dead_tup > %s", (zero,) ); 
      #
    except: 
        print('error LIVE & DEAD TUPLES!') 
  
    # store the result in data 
    #print("::::::::::::::::::::::::::::::  LIVE & DEAD TUPLES ::::::::::::::::::::::::::::::::::")
    livedead = cur.fetchall() 
    for row in livedead:
        schema = row[0]
        table = row[1]
        #print("***Schema**** "+str(row[0])+ " ***Table**** "+ str(row[1]))
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Tuple Live Relation Name "+table+" (#)\" value=\""+str(row[2])+"\" />" )
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Tuple Dead Relation Name "+table+" (#)\" value=\""+str(row[3])+"\" />" )
        #print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")

    # Average number of rows returned by one scan 
    # pay attention to tables and queries when the average number of rows go beyond million rows per scan
    try: 
        cur.execute('SELECT schemaname, relname, seq_scan, seq_tup_read, seq_tup_read / seq_scan as avg_seq_tup_read FROM pg_stat_all_tables WHERE seq_scan > 0 ORDER BY 5 DESC LIMIT 1;')
    except: 
        print('error avg_seq_tup_read!') 
  
    # store the result in data 
    #print("::::::::::::::::::::::::::::::  LIVE & DEAD TUPLES ::::::::::::::::::::::::::::::::::")
    livedead = cur.fetchall() 
    for row in livedead:
        schema = row[0]
        table = row[1]
        seq_scan = row[2]
        seq_tupread = row[3]
        #print("***Schema**** "+str(row[0])+ " ***Table**** "+ str(row[1]))
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_TABLE: Average rows by one scan Relation Name "+table+" (#)\" value=\""+str(row[4])+"\" />" )


# get the details frpm pg_stat_bgwriter
# statistics about the background writer process activity
# clean dirty pages from shared buffers to a persistent storage
def get_pg_stat_bgwriter(): 
    #Enable if above there is no connection
    # Grafana pg_stat_bgwriter_buffers_backend{instance="$instance", server="$database_server"}
    conn, cur = connect() 
  
    # select all the rows from emp 
    # Grafana (shared buffer) pg_settings_shared_buffers_bytes{instance="$instance",server="$database_server"}
    # pg_stat_bgwriter_checkpoint_write_time{instance="$instance",server="$database_server"}
    try: 
        cur.execute('SELECT buffers_alloc, buffers_checkpoint, buffers_clean, buffers_backend FROM pg_stat_bgwriter;' )
        
      #
    except: 
        print('error pg_stat_bgwriter !') 
  
    # store the result in data 
    numconn = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  BUFFER ::::::::::::::::::::::::::::::::::")
    for row in numconn:
        #print("***CHECKPOINTS**** buffers allocated  "+str(row[0]))
        print("<metric type=\"IntCounter\" name=\"Postgres| BGWRITER: Number of buffers written during checkpoints (#)\" value=\""+str(row[1])+"\" />" )
        print("<metric type=\"IntCounter\" name=\"Postgres| BGWRITER: Number of buffers written by the background writer (#)\" value=\""+str(row[2])+"\" />" )
        print("<metric type=\"IntCounter\" name=\"Postgres| BGWRITER: Number of buffers written directly by a backend (#)\" value=\""+str(row[3])+"\" />" )

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
        cur.execute("SELECT current_database(), schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, COALESCE(last_vacuum, %s) as last_vacuum, COALESCE(last_autovacuum, %s) as last_autovacuum, COALESCE(last_analyze, %s) as last_analyze, COALESCE(last_autoanalyze, %s) as last_autoanalyze, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count FROM pg_stat_user_tables where seq_scan > 25 order by seq_tup_read desc limit 1;", (dt_format,dt_format,dt_format,dt_format,) );
      #
    except: 
        print('error pg_stat_user_tables !') 
  
    # store the result in data 
    clientinfo = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  USER STATS ::::::::::::::::::::::::::::::::::")
    #print(clientinfo)
    for row in clientinfo:
        schema = row[1]
        table = row[2]
        #Last time at which this table was manually vacuumed
        last_man_vac = row[14]
        #Last time at which this table was vacuumed by the autovacuum daemon
        last_vac_auto_dae = row[15]
        #Last time at which this table was manually analyzed
        last_tab_analyze = row[16]
        #Last time at which this table was analyzed by the autovacuum daemon
        last_tab_autovac_dae = row[17]

        #print("Database: "+str(row[0])+ "***Schema**** "+str(row[1])+ " ***Table**** "+ str(row[2]))
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of sequential scans initiated on "+table+" (#)\" value=\""+str(row[3])+"\" />" ) #seq_scan
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of live rows fetched by sequential scans on "+table+"  (#)\" value=\""+str(row[4])+"\" />" ) #seq_tup_read
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of index scans initiated on "+table+" (#)\" value=\""+str(row[5])+"\" />" ) #idx_scan
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of live rows fetched by index scans on "+table+" (#)\" value=\""+str(row[6])+"\" />" ) #idx_tup_fetch
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of rows inserted on "+table+"(#)\" value=\""+str(row[7])+"\" />" ) #n_tup_ins
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of rows updated on "+table+" (#)\" value=\""+str(row[8])+"\" />" ) #n_tup_upd
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of rows deleted on "+table+" (#)\" value=\""+str(row[9])+"\" />" ) #n_tup_del
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of rows HOT updated on "+table+" (#)\" value=\""+str(row[10])+"\" />" ) #n_tup_hot_upd
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Estimated number of live rows on "+table+" (#)\" value=\""+str(row[11])+"\" />" ) #n_live_tup
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Estimated number of dead rows on "+table+" (#)\" value=\""+str(row[12])+"\" />" ) #n_dead_tup
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Estimated number of rows modified since "+table+" last analyzede (#)\" value=\""+str(row[13])+"\" />" ) #n_mod_since_analyze
        # print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Last time at which this table was manually vacuumed (#)\" value=\""+str(row[14])+"\" />" ) #last_vacuum
        # print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Last time at which this table was vacuumed by the autovacuum daemon (#)\" value=\""+str(row[15])+"\" />" ) #last_autovacuum
        # print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Last time at which this table was manually analyzed (#)\" value=\""+str(row[16])+"\" />" ) #last_analyze
        # print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Last time at which this table was analyzed by the autovacuum daemon (#)\" value=\""+str(row[17])+"\" />" ) #last_autoanalyze
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of times "+table+" has been manually vacuumed (#)\" value=\""+str(row[18])+"\" />" ) #vacuum_count
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of times "+table+" has been vacuumed by the autovacuum daemon (#)\" value=\""+str(row[19])+"\" />" ) #autovacuum_count
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of times "+table+" has been manually analyzed (#)\" value=\""+str(row[20])+"\" />" ) #analyze_count
        print("<metric type=\"IntCounter\" name=\"Postgres| STAT_USER_T: Number of times "+table+" has been analyzed by the autovacuum daemon (#)\" value=\""+str(row[21])+"\" />" ) #autoanalyze_count

        # print("Number of live rows fetched by sequential scans = ", row[4], )    #seq_tup_read
        # print("Number of index scans initiated on this table = ", row[5], )      #idx_scan
        # print("Number of live rows fetched by index scans = ", row[6], )         #idx_tup_fetch
        # print("Number of rows inserted = ", row[7], )                            #n_tup_ins
        # print("Number of rows updated = ", row[8], )                             #n_tup_upd
        # print("Number of rows deleted = ", row[9], )                             #n_tup_del
        # print("Number of rows HOT updated  = ", row[10], )                       #n_tup_hot_upd
        # print("Estimated number of live rows  = ", row[11], )                    #n_live_tup
        # print("Estimated number of dead rows  = ", row[12], )                    #n_dead_tup
        # print("Estimated number of rows modified since this table was last analyzed  = ", row[13], ) #n_mod_since_analyze
        # print("Last time at which this table was manually vacuumed   = ", row[14], )                 #last_vacuum
        # print("Last time at which this table was vacuumed by the autovacuum daemon  = ", row[15], )  #last_autovacuum
        # print("Last time at which this table was manually analyzed  = ", row[16], )                  #last_analyze  
        # print("Last time at which this table was analyzed by the autovacuum daemon   = ", row[17], ) #last_autoanalyze
        # print("Number of times this table has been manually vacuumed  = ", row[18], )                #vacuum_count
        # print("Number of times this table has been vacuumed by the autovacuum daemon  = ", row[19], ) #autovacuum_count
        # print("Number of times this table has been manually analyzed  = ", row[20], )                 #analyze_count
        # print("Number of times this table has been analyzed by the autovacuum daemon  = ", row[21], ) #autoanalyze_count
        # print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")


# get the details frpm pg_stat_replication
def get_pg_stat_replication(): 
    #Enable if above there is no connection
    conn, cur = connect() 

    cur.execute("SELECT pg_is_in_recovery();")
    repl_status = cur.fetchone()
    if repl_status[0] is True:
        # Logic for Replica PostgreSQL
        try: 
            cur.execute("select round((extract(epoch from now()) - extract(epoch from last_msg_send_time)) * 1000) as delay_ms, conninfo from pg_stat_wal_receiver;"  )
            #cur.execute("select * from pg_stat_wal_receiver;"  )
        except: 
            print('error get_pg_stat_replication !') 
    
        # store the result in data 

        replicainfo = cur.fetchall() 
        #print("::::::::::::::::::::::::::::::  REPLICATION REPLICA ::::::::::::::::::::::::::::::::::")
        for row in replicainfo:
            delay_ms = str(int(row[0]))
            conn_info = row[1]
            start='host='
            end=' '
            master_server = (conn_info.split(start))[1].split(end)[0]

            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Replica with Master "+master_server+" Delays in Mili Secs (#)\" value=\""+delay_ms+"\" />" )

    else:
        # Logic for Master PostgreSQL
        # select all the rows from get_pg_stat_replication 
        #dt_format='1970-01-01Z'
        try: 
            cur.execute("SELECT application_name, client_hostname, backend_start, state, write_location - sent_location AS write_lag, flush_location - write_location AS flush_lag, replay_location - flush_location AS replay_lag  FROM pg_stat_replication;"  )
        
        except: 
            print('error get_pg_stat_replication !') 
    
        # store the result in data 
        replicationinfo = cur.fetchall() 
        #print("::::::::::::::::::::::::::::::  REPLICATION CLIENT INFO ::::::::::::::::::::::::::::::::::")
        for row in replicationinfo:
            app_name = row[0]
            replica_host = row[1]
            time_wal_sender = str(row[2])
            wal_state = row[3]
            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Master Write Lag with "+app_name+" and "+replica_host+" status "+wal_state+" (#)\" value=\""+str(row[4])+"\" />" )
            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Master Flush Lag with "+app_name+" and "+replica_host+" status "+wal_state+" (#)\" value=\""+str(row[5])+"\" />" )
            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Master Replay Lag with "+app_name+" and "+replica_host+" status "+wal_state+" (#)\" value=\""+str(row[6])+"\" />" )

        
        # LAG In Bytes pg_replication_slots;
        try:
            #cur.execute("SELECT restart_lsn() - confirmed_flush_lsn FROM pg_replication_slots;")
            cur.execute("SELECT count(*) FROM pg_replication_slots where active=\'t\';")
        except:
            print("error pg_replication_slots; !")

        replicationslot = cur.fetchall()   
        for row in replicationslot:
            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Number of Active replication slots (#)\" value=\""+str(row[0])+"\" />" )

        # Inactive replication Slots
        try: 
            cur.execute('SELECT count(*) FROM pg_replication_slots WHERE NOT active; ') 
        #
        except: 
            print('error Inactive replication slots !') 
    
        # store the result in data 
        inreplslots = cur.fetchall() 
        for row in inreplslots:
            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Inactive replication slots (#)\" value=\""+str(row[0])+"\" />" )

        # Lag Bytes
        try: 
            cur.execute('SELECT pg_xlog_location_diff(pg_current_xlog_insert_location(), flush_location) AS lag_bytes FROM pg_stat_replication where application_name = \'awx\'; ') 
        #
        except: 
            print('error Lag Bytes in Replication !') 
    
        # store the result in data 
        lagbytes = cur.fetchall() 
        for row in lagbytes:
            print("<metric type=\"IntCounter\" name=\"Postgres| Replication: Master Lag Bytes (#)\" value=\""+str(row[0])+"\" />" )

def get_pg_statio_all_tables(): 
    #Enable if above there is no connection
    conn, cur = connect() 
  
    # select all the rows from emp 
    try: 
        cur.execute('SELECT current_database(), schemaname, relname, heap_blks_read FROM pg_statio_all_tables order by heap_blks_read desc fetch first 1 rows only; ') 
      #
    except: 
        print('error get_pg_statio_all_tables !') 
  
    # store the result in data 
    mostdiskusage = cur.fetchall() 
    #print("::::::::::::::::::::::::::::::  DISK USAGE ::::::::::::::::::::::::::::::::::")
    for row in mostdiskusage:
        #print("Database: "+str(row[0])+ "***Schema**** "+str(row[1])+ " ***Table**** "+ str(row[2]))
        print("<metric type=\"IntCounter\" name=\"Postgres| ALL_TABLE: Table with most DISK Usage (#)\" value=\""+str(row[3])+"\" />" )

def get_pg_table_size(): 
    #Enable if above there is no connection
    conn, cur = connect() 
    try: 
        cur.execute('SELECT schema_name,relname,pg_size_pretty(table_size) AS size,table_size FROM ( SELECT pg_catalog.pg_namespace.nspname AS schema_name,relname,pg_relation_size(pg_catalog.pg_class.oid) AS table_size FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid) t WHERE schema_name NOT LIKE \'pg_%\' ORDER BY table_size DESC limit 5; ') 
    except: 
        print('error get_pg_table_size !') 
    table_size = cur.fetchall() 
    for row in table_size:
        schema = row[0]
        table = row[1]
        size = row[2]
        print("<metric type=\"IntCounter\" name=\"Postgres| TABLE_SIZE: Size on Schema "+schema+" Table "+table+" (#)\" value=\""+str(row[3])+"\" />" )

    if conn is not None:
        conn.close()



suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
def human_size(nbytes):
    human = nbytes
    rank = 0
    if nbytes != 0:
        rank = int((math.log10(nbytes)) / 3)
        rank = min(rank, len(suffixes) - 1)
        human = nbytes / (1024.0 ** rank)
    f = ('%.2f' % human).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[rank])

  
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

    #pg_pg_table_size 
    get_pg_table_size()         

