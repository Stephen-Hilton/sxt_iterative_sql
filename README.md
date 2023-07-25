# sxt_iterative_sql
Connect to SXT and do an iterative data pull, generating one file per iterative pull.    Requires a PK on the table or view, which allows the process to order the dataset and pull the dataset into various files. 

Includes 2 main functions: iterExtract and iterMerge.   

## iterExtract
Iteratively retrieves data into a collection of files, one per "rows_per_file" records.

## iterMerge
Iteratively loops thru files produced in iterExtract and merges into a single return file. 

## iterSQL_Request Object
Both functions require a iterSQL_Request json object that looks like:


```
sample_json = [ 
    {'name': 'New Query Request'
    ,'envfile': '.env'
    ,'resourceid': 'ETHEREUM.TRANSACTIONS'
    ,'rows_per_file': 2000
    ,'pkname': 'OrderCol'
    ,'pklogic': "cast(Time_Stamp as varchar(25)) ||'.'|| cast(Block_Number as varchar(12)) || cast(Transaction_Fee as varchar(24))"
    ,'folderpath': './output'
    ,'filename': 'ETHEREUM_TRANSACTIONS_Filtered3.{i}.csv'
    ,'sql':"""  SELECT TRANSACTION_HASH, FROM_ADDRESS, {pkcolumn}
                FROM ETHEREUM.TRANSACTIONS 
                WHERE TIME_STAMP >= '2023-01-01T00:00:00.000+00:00' 
                and TO_ADDRESS IN ('0xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd')
                {and_where} {order_by} {limit_n}"""
    }
]
```

When authoring the SQL, you will need to add the following, wrapped in {curly_brackets}:
- {pkcolumn} = will be constructed from "{pklogic} as {pkname}", and is required so the process can track the last row saved
- {and_where} = constructed as an "AND {pklogic} > " the last {pkcolumn} from the last pull
- {order_by} = constructed from an "ORDER BY {pkname}" to ensure the dataset order
- {limit_n} = constructed from an "LIMIT {rows_per_file}" to limit to a specific size

After each {rows_per_file} rows, the SQL is re-run, so this is FAR from the most efficient approach.  The process will attempt to re-run after failures, although will wait 5min between re-try attempts (to prevent an automated bl

You can submit multiple requests, although the above only shows one.
