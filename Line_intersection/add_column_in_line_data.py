
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

    query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'line') ORDER BY TABLE_NAME """
    sqlExecuter.execute(query)
    all_tables = sqlExecuter.fetchall()  
    for table_name in all_tables:
        
            file_name = table_name[0]
            query = f""" alter table line."{file_name}" add column if not exists drp_road_id character varying,
            add column  if not exists
            drp_road_name character varying, add column  if not exists drp_road_category character varying """
            sqlExecuter.execute(query)
            connection.commit()
            print(file_name)

    print("total = ", len(set(all_tables))) 
    print("added id in columns ")


def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()

