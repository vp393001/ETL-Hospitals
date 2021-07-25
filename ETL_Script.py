def try_parsing_date(date, format):
    try:
        if format == 1:
            # YYYYMMDD format
            return datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
        else:
            # DDMMYYYY format
            return datetime(year=int(date[4:8]), month=int(date[2:4]), day=int(date[0:2]))
    except ValueError:
        return -1

def main(open_date, last_consulted_date, date_of_birth):
    invalid_data = []
    file_detail_handle = open('C:/Users/vparmar/Downloads/Incubyte/file_detail.txt', 'r')
    file_handle = open('C:/Users/vparmar/Downloads/Incubyte/file.txt', 'r')

    # Skip headers
    next(file_detail_handle)
    next(file_handle)

    # Building staging table creation query
    query1 = 'Create table staging ( '
    rows = file_detail_handle.readlines()
    for row in rows:
        row = row.strip().split('|')

        # Append column name
        query1 += '\"{}\" '.format(row[1])

        # Append data type
        if row[3] in ['VARCHAR', 'CHAR']:
            query1 += row[3] + '({})'.format(row[2])
        else:
            query1 += row[3]

        # Primary key check
        if row[5] == 'Y':
            query1 += ' PRIMARY KEY '

        # Mandatory column check
        if row[4] == 'Y':
            query1 += ' NOT NULL '

        query1 += ', '
    query1 = query1.rsplit(',', 1)[0] + ' )'
    # print(query1)

    # Building staging table insert query
    query2 = 'Insert into staging values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    rows = file_handle.readlines()
    data = []
    for row in rows:
        row = row.strip().split('|')
          
        # Checking whether mandatory columns data is present or not
        if '' in [i.strip() for i in [row[1], row[2], row[3]]]:
            invalid_data.append(row)
            continue

        # Parse date
        for i, format in zip([3,4,9], [open_date, last_consulted_date, date_of_birth]):
            date = try_parsing_date(row[i], format)
            if date != -1:
                row[i] = date
            else:
                invalid_data.append(row)
                continue

        data.append(tuple(row[1:]))
    # print(data)

    query3 = 'Select distinct("Country") from staging'
    connection = None

    try:
        connection = psycopg2.connect(user="",
                                    password="",
                                    host="localhost",
                                    port="5432",
                                    database="Hospitals")

        cursor = connection.cursor()
        cursor.execute(query1)
        cursor.executemany(query2, data)
        cursor.execute(query3)
        country_list = [i[0].strip() for i in cursor.fetchall()]

        for country in country_list:
            # Create country wise tables and split data
            query4 = 'Create table \"{}\" as (select * from staging where "Country" = \'{}\' )'.format('Table_' + country, country)
            cursor.execute(query4)

        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error from PostgreSQL:", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    # Write rows having missing data in a file
    missing_file_handle = open('invalid_data.txt', 'w')
    for row in invalid_data:
        missing_file_handle.write(str(row) + '\n')

    file_detail_handle.close()
    file_handle.close()
    missing_file_handle.close()
    
if __name__ == '__main__':
    import psycopg2
    from datetime import datetime 

    print("Select date format for each column: \n Press 1 for YYYYMMDD \n Press 2 for DDMMYYYY")
    open_date = int(input("Open Date: "))
    last_consulted_date = int(input("Last Consulted Date: "))
    date_of_birth = int(input("Date of Birth: "))
     
    main(open_date, last_consulted_date, date_of_birth)