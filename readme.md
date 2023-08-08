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
            "Table 1",
            "Table 2",
        ]
    }





## TODOS
- restructure logging system using python documentation (rn pretty quick n' not how it's supposed to be used) **DONE**
- try using pandas to_sql function to load data, rn not working due to   precision error would need to use sqalchemy (large restructuring of code) 
- better file_name mapping/naming convention 
    - some file paths are duplicate/not saved in variables causing troubles when editing the code
- implement tests
- setup LastUpdate Table so that it automatically gets created if it's missing 