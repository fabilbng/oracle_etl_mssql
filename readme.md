# Oracle Pipeline 

**ETL-Pipeline von Oracle DB zu MSSQL**

## What you need
### .ENV file with the following information
    ENV = "DEV OR PROD"
    ORACLE DB = exampleIP/Oracle
    ORACLE UN = ExampleUsername
    ORACLE PW = ExamplePasswort
    MSSQL_DBS = exampledatabaseserverIP
    MSSQL_DB = exampledatabase
    MSSQL_UN = exampleUsername
    MSSQL_PW = examplePasswort 



### settings.json file
    {
        "tables" : [
            {
                "name": "TableName"
                "exclude_columns": [
                    "Column1",
                    "Column2"
                ]
            }
        ]
    }


**Exclude Columns Description:** If the array includes column names, the script will still fetch it from the oracle db and safe it in the raw folder. During the transform step, the script will remove these columns from the dataset and safe it in the transformed path, so they won't be inserted in the load step. 

The columns will still be there on the mssql table as the table synchronisation is a different step using table info data that it fetches from somewhere else, but they are all NULL from the moment you you exclude the column. I don't see it neccessary to remove these columns, since that would also mess with the column synchronisation.  




## TODOS
- restructure logging system using python documentation (rn pretty quick n' not how it's supposed to be used) **DONE**
- try using pandas to_sql function to load data, rn not working due to   precision error would need to use sqalchemy (large restructuring of code) 
- better file_name mapping/naming convention 
    - some file paths are duplicate/not saved in variables causing troubles when editing the code
- implement tests
- setup LastUpdate Table so that it automatically gets created if it's missing 