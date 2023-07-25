import pySXT, iterExtract, iterMerge

# expected .env parameters: 
# API_URL="https://api.spaceandtime.app"
# USERID=Stephen
# USER_PUBLIC_KEY ="abcdefghijklmnopqrstuvwxyz12345678912345678="
# USER_PRIVATE_KEY="abcdefghijklmnopqrstuvwxyz12345678912345678="

iterSQL_requests = \
[ 
    {'name': 'Joshs Query Request 2023-07-24'
    ,'envfile': '.env'
    ,'resourceid': 'ETHEREUM.TRANSACTIONS'
    ,'rows_per_file': 2000
    ,'pkname': 'OrderCol'
    ,'pklogic': "cast(Time_Stamp as varchar(25)) ||'.'|| cast(Block_Number as varchar(12)) || cast(Transaction_Fee as varchar(24))"
    ,'folderpath': './output'
    ,'filename': 'ETHEREUM_TRANSACTIONS_Filtered3.{i}.csv'
    ,'sql':"""  SELECT TRANSACTION_HASH, FROM_ADDRESS, {pkcolumn}
                FROM ETHEREUM.TRANSACTIONS 
                WHERE TIME_STAMP >= '2020-12-22T10:22:54.000+00:00' 
                and TO_ADDRESS IN ('0xcc9a0B7c43DC2a5F023Bb9b738E45B0Ef6B06E04')
                {and_where} {order_by} {limit_n}"""
    }
]


iterExtract.iter_data_pull(iterSQL_requests)
iterMerge.merge_files(iterSQL_requests)
print('Done!')
