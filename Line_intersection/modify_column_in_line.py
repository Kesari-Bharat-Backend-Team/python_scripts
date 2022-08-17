
import psycopg2

# hostname = 'localhost'  
# username = 'postgres'
# password = 'EiOiJja3ZqaTlrcnQyNzh'
# database = 'kesari_bharat'
# port = 5432


hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432


def myQuery(connection):

    sqlExecuter = connection.cursor()
    schema_name = "line"
    query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'line') ORDER BY TABLE_NAME """
    sqlExecuter.execute(query)
    all_tables = sqlExecuter.fetchall()  

    for table_name in all_tables:
            file_name = table_name[0]

            query = f''' 
            alter table if exists {schema_name}."{file_name}"
                alter column drp_road_id type character varying;
            '''
            sqlExecuter.execute(query)
            connection.commit()
            print("we are updating", file_name)




def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
