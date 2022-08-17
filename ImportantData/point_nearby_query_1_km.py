import psycopg2

hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432

def myQuery(connection):

    sqlExecuter = connection.cursor()

    query = f"select id, hab_name, latitude, longitude from try_admin_point.habitant where flag = 0 and id between 1 and 100"
    sqlExecuter.execute(query)
    records = sqlExecuter.fetchall() 
    hash = dict() 

    for data in records:

        id, habitant_name, latitude, longitude = data 
        habitant_name = habitant_name.replace("'", "")
        habitant_name = habitant_name.replace('"', "")
        query = f"update try_admin_point.habitant set flag = 1 where id = {id}"
        sqlExecuter.execute(query)
        connection.commit() 

        query = f"""
        select my_id, name, latitude, longitude  from try_admin_point.hamlet_village_mixture 
            where ST_DWithin(
            'POINT( {longitude} {latitude})'::geography,
                ST_MakePoint(longitude, latitude), 1000
            ); 
        """
        
        sqlExecuter.execute(query)
        hamlet_village_names = sqlExecuter.fetchall()
        
        key = f'[{id}, "{habitant_name}"]'

        nearby_hamlet_village_names = [] 

        for data in hamlet_village_names:

            my_id, name, latitude, longitude = data 
            name = name.replace("'", "").replace('"', "")
            nearby_hamlet_village_names.append({my_id : name})
                
        if nearby_hamlet_village_names == []:
            print("no nearby village/hamlet name ", id)
            continue
       
        nearby_hamlet_village_names = str(nearby_hamlet_village_names)
        nearby_hamlet_village_names = nearby_hamlet_village_names.replace("'", '"')
        
        # if len(nearby_hamlet_village_names) > 250:
        #     hash[key] = nearby_hamlet_village_names
        #     print("hash data = ", nearby_hamlet_village_names)
        #     continue

        query = f"""
            insert into try_admin_point.nearby_habitant_village_and_hamlet_1KM
            (habitant_key_value_pair, hamlet_village_pair) 
            values('{key}', '{nearby_hamlet_village_names}' )
        """
        sqlExecuter.execute(query)
        connection.commit() 
        print("pnbq1km -> inserted id = ", id)

    print("Done!!!")


def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()