import psycopg2

hostname = 'localhost'  # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432

def myQuery(connection):

    sqlExecuter = connection.cursor()
    schema_name = "point_intersection_duplicate_delete"
    # query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'point_intersection_duplicate_delete')"""
    # sqlExecuter.execute(query)
    # all_tables = sqlExecuter.fetchall() 
    # all_tables = [1] 
    # for table_name in all_tables:

            # file_name = table_name[0]
            # print(file_name)
    file_name = '25'
    if True:

            query = f''' 
                SELECT road_new_i FROM point_intersection_duplicate_delete."{file_name}"
                where path like 'osm%' and path_2 like 'face%'
                group by road_new_i
                having count(*) > 1 
                limit 5 
            '''
            sqlExecuter.execute(query)
            records = sqlExecuter.fetchall() 

            for data in records:

                road_new_i = data[0] 
                print(file_name, road_new_i)
                # query = f"""
                    
                #     select * from line."{file_name}"
                #     where road_new_i = {road_new_i}
                
                # """
                # sqlExecuter.execute(query)
                


def connect_to_Database():

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()