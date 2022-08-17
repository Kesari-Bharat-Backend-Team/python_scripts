import psycopg2

hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432


def myQuery(connection):

    sqlExecuter = connection.cursor()

    query = f"""select id, latitude, longitude from try_admin_point.hamlet"""
    sqlExecuter.execute(query)
    records = sqlExecuter.fetchall()

    for data in records:
        my_id, latitude, longitude = data

    #     query = f"""
    #     update try_admin_point.hamlet
    # set geog = ST_GeographyFromText('POINT({latitude} {longitude})') 
	# where id = {my_id};
    #     """
        query = f"""
        update try_admin_point.hamlet
    set geog = ST_MakePoint({latitude},{longitude})::geography
	where id = {my_id};
        """
        try:
            sqlExecuter.execute(query)
            connection.commit() 
            print("my_id = ", my_id, "updated Scucessfully !!!")
        except Exception as e:
            print(str(e))
            print("my_id = ", my_id)
            # continue 
            return 


def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()


connect_to_Database()
