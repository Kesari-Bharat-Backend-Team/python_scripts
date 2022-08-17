
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
    
    query = """ 

    select osm_id from public."0" 
    where osm_id is not null 
    """
    sqlExecuter.execute(query)
    records = sqlExecuter.fetchall() 

    for data in records:

        osm_id = data[0]

        query = f""" 

            select b_raod_new_i from public."0" 
            where b_path like 'face%' and osm_id = '{osm_id}'
            group by b_raod_new_i
            having count(*) > 1 
            limit 10
        """
        sqlExecuter.execute(query)
        intersect_records = sqlExecuter.fetchall() 
        if intersect_records:
            print(intersect_records, "\n" * 5)
            return 
        print(".", end = " ")

    print("Done!!!")

def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
