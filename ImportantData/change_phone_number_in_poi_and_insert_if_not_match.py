import psycopg2

hostname =  'localhost' # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432


def myQuery(connection):

    sqlExecuter = connection.cursor()
  
    table_path = "excel_data.mixed_data"

    query = f"select id, category, name, address,email_address, phone_number, website,rating, reviews, latitude, longitude, district_no from {table_path} where flag = 0"
    
    sqlExecuter.execute(query)
    records = sqlExecuter.fetchall()
    print("len = ", len(records))

    for data in records:
        id, category, name, address, email_address, phone_number, website,rating, reviews, latitude, longitude, district_no = data 

        reviews = float(reviews) if reviews else 0.0
        if not district_no:
            continue

        query = f"update {table_path} set flag = 1 where id = {id}" 
        sqlExecuter.execute(query)
        connection.commit() 

        #Matching data into poi_table 

        query = f"""select id from jan_2022_poi.poi_{district_no}
         
         where (name = '{name}' and address = '{address}' and latitude = '{latitude}' and longitude = '{longitude}')
         
         """
        query = query.replace("None", "null")
        sqlExecuter.execute(query)
        records = sqlExecuter.fetchall()

        if records:
            print("we have matching data id = ", id, 'poi_table = ', district_no)
            #updating  email, phone, webseite 

            for poi_id in records:
                
                poi_id = poi_id[0]
                query  = f"""update jan_2022_poi.poi_{district_no} 
                    set email = '{email_address}',
                        website = '{website}' 
                        'phone_1' = '{phone_number}'
                where id = {poi_id}"""
                query = query.replace("None", "null")

                sqlExecuter.execute(query)
                connection.commit() 

        else:
            #data not matching
            #inserting data in poi_table 

            print("we don't have match data. Excel_id = ", id, "in poi_", district_no)
            query = f"insert into jan_2022_poi.poi_{district_no}  (category, name, address,email, phone_1, website,rating, reviews, latitude, longitude) values ('{category}', '{name}', '{address}','{email_address}', '{phone_number}', '{website}','{rating}', '{reviews}', '{latitude}', '{longitude}')"
            query = query.replace("None", "null")
            sqlExecuter.execute(query)
            connection.commit() 

def connect_to_Database():

    myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )
    myQuery( myConnection )
    myConnection.close()

connect_to_Database()
