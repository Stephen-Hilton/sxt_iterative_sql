# sxt_iterative_sql
Connect to SXT and do an iterative data pull, generating one file per iterative pull.    Requires a PK on the table or view, which allows the process to order the dataset and pull the dataset into various files. 

`
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
`