import pySXT 
import os, time

sample_json = [ {'name': 'Name of my query'
                ,'resourceid': 'ETHEREUM.TRANSACTIONS'
                ,'rows_per_file': 2000
                ,'pkcolumn': 'OrderCol'
                ,'pklogic': "cast(Time_Stamp as varchar(25)) ||'.'|| cast(Block_Number as varchar(12)) || cast(Transaction_Fee as varchar(24))"
                ,'folderpath': './output'
                ,'filename': 'ETHEREUM_TRANSACTIONS_Filtered3.{i}.csv'
                ,'sql':"""  SELECT TRANSACTION_HASH, FROM_ADDRESS 
                            FROM ETHEREUM.TRANSACTIONS 
                            WHERE TIME_STAMP >= '2023-06-22T10:22:54.000+00:00' 
                            and TO_ADDRESS IN ('0xcc9a0B7c43DC2a5F023Bb9b738E45B0Ef6B06E04') """
                }
              ]


def iter_data_pull(iterSQL_Requests={}):
    for sqlObject in iterSQL_Requests:
        sqlName = sqlObject['name']
        print(f'--------- PROCESSING {sqlName} ---------')
        pkname = sqlObject['pkname']
        pklogic = sqlObject['pklogic']
        resourceid = sqlObject['resourceid']
        sqltemplate = sqlObject['sql']
        filenametemplate = sqlObject['filename']
        folderpath = sqlObject['folderpath']
        rows_per_query = sqlObject['rows_per_file']
        headers = None
        where = ''
        i=1

        sxt = pySXT.sxt(envfile = sqlObject['envfile'])

        while True:
            if sxt.reauth_soon(): 
                print('connection timing out soon, reconnecting...')
                success, token, refresh, reauthTS = sxt.authenticate()
            print(f'  iteration {str(i).rjust(4,"0")} started')
            
            sql = sxt.beautify_query(sqltemplate)
            for f, r in {'and_where':where, 'pkcolumn': f'{pklogic} as {pkname}', 'limit_n': f' LIMIT {rows_per_query}', 'order_by': f' ORDER BY {pkname}'}.items():
                sql = sql.replace('{'+f+'}',r) 
            filename = '/'.join([folderpath, filenametemplate.replace('{i}', str(i).rjust(6,"0")) ])
            status_code = False 

            while status_code != 200:
                status_code, json_results = sxt.query_dql(resourceId=resourceid, sql=sql)
                if status_code != 200:  
                    success, token, refresh, reauthTS = sxt.reauth_ifneeded(print_msg='connection timing out soon, reconnecting...')
                    print('\n'.join([json_results['reason'], json_results['error'], 'Sleeping for 5min...']))
                    time.sleep(300) # sleep 5min on failure
            if len(json_results) == 0: break

            headers = ', '.join(json_results[0].keys())

            with open(filename, "w") as fh: # JSON to CSV:
                fh.write(f"{headers}\n") 
                for row in json_results: 
                    fh.write( ', '.join([f'"{d}"' for d in row.values()]) + '\n' )
                
            where = f"and {pklogic} > '{row[pkname]}'"
            i+=1
            print(f'                 complete')

    print("DATA PULL COMPLETE!!!")
    return iterSQL_Requests
