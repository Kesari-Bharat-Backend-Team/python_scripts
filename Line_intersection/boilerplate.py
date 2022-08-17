
import psycopg2

# hostname = 'localhost'  
# username = 'postgres'
# password = 'EiOiJja3ZqaTlrcnQyNzh'
# database = 'kesari_bharat'
# port = 5432


hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = '$Ks@123'
database = 'Kesari_bharat'
port = 5432


def myQuery(connection):

    sqlExecuter = connection.cursor()
    schema_name = "point_intersection_roadname"
    # query = f"where flag = 0"
    # query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'point_intersection_roadname') ORDER BY TABLE_NAME """
    # sqlExecuter.execute(query)
    # all_tables = sqlExecuter.fetchall()  

    # for table_name in all_tables:
    #         file_name = table_name[0]

    for i in range(109, 110):

        file_name = "109"
        print(file_name)

        query = f""" select drrp_road_, roadcatego, roadname, road_new_i FROM
		{schema_name}."{file_name}"
        where intersection_flag = 0
		group by road_new_i, drrp_road_, roadcatego, roadname
		having count(*) > 1  ;
        """

        sqlExecuter.execute(query)
        records = sqlExecuter.fetchall() 

        for data in records:

            drp_road_id, drp_road_category, drp_road_name, road_new_i = data 

            road_new_i = int(road_new_i)

            query = f""" update line."{file_name}"  
                set
                    drp_road_id = '{drp_road_id}',
                    drp_road_category = '{drp_road_category}',
                    drp_road_name = '{drp_road_name}'
                where road_new_i = {road_new_i}
            """
            # print('query = ', query)
            sqlExecuter.execute(query)

            query = f"""update {schema_name}."{file_name}"  set intersection_flag = 1"""
            sqlExecuter.execute(query)
            print("we are updating ", road_new_i)



def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
