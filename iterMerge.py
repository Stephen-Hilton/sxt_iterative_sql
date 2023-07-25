import os, time, re 

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


def merge_files(iterSQL_Requests={}):
        
    for sqlObject in iterSQL_Requests:

        print(f'--------- ITERATIVE MERGE FOR TABLE {sqlObject["filename"] } ---------')
        subfolderpath = sqlObject['folderpath']
        tablename_regex_pattern = sqlObject['filename']

        # Destination Filename:
        dest_filename = f'allrows.{tablename_regex_pattern}'.replace('{i}','').replace('..','.')

        # GET ALL FILES IN PATH THAT CONTAIN table
        for path, dirs, files in os.walk(subfolderpath):
            
            fhall = open(f'./{subfolderpath}/{dest_filename}', 'w')
            first_iteration = True

            for file in files: 
                if re.match(tablename_regex_pattern.replace('{i}','.*'), file ) and 'allrows' not in file: 
                    print(f'  Merging {file}...')
                    filepath =  f'{path}/{file}'
                    with open(filepath, 'r') as fh:
                        lines = fh.readlines()
                        if first_iteration: 
                            fhall.writelines(lines)
                            first_iteration = False
                        else:
                            fhall.writelines(lines[1:])
            fhall.close()
            break # don't recurse
            
        print(f'DONE with merge process for {dest_filename} ---------')
    print('DONE with ALL merge processes!')
    return iterSQL_Requests


