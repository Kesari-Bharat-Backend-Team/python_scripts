import psycopg2

hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432

def myQuery(connection):

    sqlExecuter = connection.cursor()

    schema_name = 'try_admin_point'
    file_name = 'villages_2019'

    query = f'''select gid, vilnam_soi, stcode11, dtcode11, sdtcode11, vilcode11, stname, dtname, sdtname, latitude, longitude from {schema_name}.{file_name} where flag = 0
    '''
    sqlExecuter.execute(query)
    records = sqlExecuter.fetchall() 

    for data in records:
        # print(data)
        gid, village_name, state_code, district_code, sub_district_code, village_code, state_name, district_name, sub_district_name, latitude, longitude  = data 

        # village_name, state_code, district_code, sub_district_code

        village_name = village_name.replace("'", "") if village_name else village_name 
        sub_district_name = sub_district_name.replace("'", "") if sub_district_name else sub_district_name 
        data_source = 'village_point'

        data_values = tuple([village_name, village_name, state_code, district_code, sub_district_code, village_code, state_name, district_name, sub_district_name, latitude, longitude, data_source])

        query = f"""
                insert into {schema_name}.hamlet_village_mixture 
                (name, village_name, state_code, district_code, sub_district_code, village_code, state_name, district_name, sub_district_name, latitude, longitude, data_source) values {data_values}
            """
        query = query.replace("None", "null")
        # print("query = ", query)
        sqlExecuter.execute(query)
        connection.commit() 


        query = f"""
                update {schema_name}.{file_name} set flag = 1 where gid = {gid}
        """

        sqlExecuter.execute(query)
        connection.commit() 

        print("file_name = ", file_name, "is running", " ", gid)



        




def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
