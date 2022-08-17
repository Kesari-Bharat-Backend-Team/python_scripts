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
    for table_name in all_tables:

            file_name = table_name[0]
            if file_name in ['385', '506', '514', '527', '122', '364', '685', '75']: continue
            print(file_name)
            query = f""" select drrp_road_, roadcatego, roadname, road_new_i FROM
            {schema_name}."{file_name}"
            where intersection_flag = 0
            group by road_new_i, drrp_road_, roadcatego, roadname
            having count(*) > 1;
            """

            sqlExecuter.execute(query)
            records = sqlExecuter.fetchall() 
            for data in records:

                drp_road_id, drp_road_category, drp_road_name, road_new_i = data 

                if road_new_i is None:
                    print("road id is None", data)
                    continue 
                
                road_new_i = int(road_new_i)

                query = f""" update line."{file_name}"  
                    set
                        drp_road_id = '{drp_road_id}',
                        drp_road_category = '{drp_road_category}',
                        drp_road_name = '{drp_road_name}'
                    where road_new_i = {road_new_i}
                """
                sqlExecuter.execute(query)
                connection.commit()

                query = f"""update {schema_name}."{file_name}"  set intersection_flag = 1 where road_new_i = {road_new_i}"""
                sqlExecuter.execute(query)
                connection.commit()
    print("Done!!!")


def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()