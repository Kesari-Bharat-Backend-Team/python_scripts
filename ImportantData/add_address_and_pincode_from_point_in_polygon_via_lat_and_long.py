import psycopg2

hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432

def myQuery(connection):

    sqlExecuter = connection.cursor()
    schema = "jan_2022_poi"
    
    for i in range(375, 736):
        
        file_name = f"poi_{i}"
        # file_name = 'copy_jaipur'
        query = f"select latitude, longitude, id from {schema}.{file_name} where pincode is null"
        
        sqlExecuter.execute(query)
        records = sqlExecuter.fetchall() 
        
        for data in records:
            
            latitude, longitude, id = data 
            
            query = f""" 
                select pincode, officename, stname, dtname, sdtname from polygon.pincode_or_locality
                where 
                
                ST_intersects( ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326), polygon.pincode_or_locality.geom ) """
            sqlExecuter.execute(query)
            pincode_records  = sqlExecuter.fetchall()

            for  pincode_data in pincode_records:
                
                pincode, locality_name, state_name, district_name, sub_district_name = pincode_data
                
                locality_name = locality_name.replace("'", "") if locality_name else locality_name
                state_name = state_name.replace("'", "") if state_name else state_name
                district_name = district_name.replace("'", "") if district_name else district_name
                sub_district_name = sub_district_name.replace("'", "") if sub_district_name else sub_district_name
                
                print(f"in {file_name} we don't have pincode. plz check id = " , id)
                print(f"now we are updating {file_name} id = {id} and set pincode = {pincode}")

                query = f""" UPDATE {schema}.{file_name}
                        SET 
                        pincode = {pincode},
                        locality_name = '{locality_name}', 
                        state_name = '{state_name}',
                        district_name = '{district_name}',
                        sub_district_name = '{sub_district_name}'
                        WHERE id = {id}    
                """
                query = query.replace("None", "null")
                sqlExecuter.execute(query)
                connection.commit() 
    
    print("Successfully added Pincode in ", file_name)
            


def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
