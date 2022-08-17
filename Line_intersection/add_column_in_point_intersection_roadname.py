import psycopg2

hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432

def myQuery(connection):

    sqlExecuter = connection.cursor()
    schema_name = "point_intersection_roadname"
    query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'point_intersection_roadname')"""
    sqlExecuter.execute(query)
    all_tables = sqlExecuter.fetchall()  
    counter = 0 
    for table_name in all_tables:
            counter += 1 
            file_name = table_name[0]
            # query = f""" alter table  {schema_name}."{file_name}"
            #         add column if not exists intersection_flag integer default 0;
            #  """
            query = f""" update {schema_name}."{file_name}" set intersection_flag = 0 """
            sqlExecuter.execute(query)
            print("updated flag ", file_name, "counter = ", counter)
            # print("addded flag ", file_name)
            connection.commit()
    print("Done!!!")

def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
