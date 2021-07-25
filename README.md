# ETL-Hospitals

Step 1: 
  - Get table column related details from `file_detail.txt`
  - Build staging table create query
    - `Create table staging ( "Customer Name" VARCHAR(255) PRIMARY KEY  NOT NULL, 
      "Customer ID" VARCHAR(18) NOT NULL, 
      "Customer Open Date" DATE NOT NULL, 
      "Last Consulted Date" DATE, 
      "Vaccination Type" CHAR(5), 
      "Doctor Consulted" CHAR(255), 
      "State" CHAR(5), "Country" CHAR(5), 
      "Date of Birth" DATE, 
      "Active Customer" CHAR(1) )`
      
Step 2:
  - Extract, Transform and Load data into intermediate staging table
    - Read rows line by line from the data file `file.txt`
    - Check for missing value in Customer Name, Customer ID or Customer Open Date columns.
      - If whitespace or empty string is present then don't insert those rows in staging table and append them into invalid_data list
    - Try to parse date formats in YYYYMMDD or DDMMYYYY formats
      - Column wise date format was taken from the user earlier
      - Try to parse date with appropriate format
      - If it is throwing exception in parsing then don't insert those rows in staging table and append them into invalid_data list
    
Step 3:
  - Get distinct countries values from the staging table
    - `Select distinct("Country") from staging`

Step 4:
  - Create country wise tables and split data
    - `Create table "Table_USA" as (select * from staging where "Country" = 'USA')`

Step 5: 
  - Write rows having missing data in mandatory columns or having date parsing error in `invalid_data.txt` file
