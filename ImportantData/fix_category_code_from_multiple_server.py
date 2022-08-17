from fileinput import filename
import psycopg2

def myQuery(connection1, connection2):
    print("making connection")
    sqlExecuter_old_server = connection1.cursor()
    sqlExecuter_new_server = connection2.cursor()
    query = """select name, latitude, longitude, id geom from jan_2022_poi.poi_109 
        where category_code like '600-6500-0073'
    limit 1;"""  
    sqlExecuter_new_server.execute(query)
    records = sqlExecuter_new_server.fetchall() 
    
    for data in records:
        print(data)
    



def connect_to_Database():
    print("we are calling myQuery")
    connection1 = psycopg2.connect( host='localhost', user='ubuntu', password='Sm@637638', dbname='Kesari_bharat', port =  5432)
    connection2 = psycopg2.connect( host='3.6.253.136', user='postgres', password='EiOiJja3ZqaTlrcnQyNzh', dbname='kesari_bharat', port =  5432)

    print("we have connected sucessfully.")
    myQuery( connection1, connection2)
    connection1.close()
    connection2.close()

connect_to_Database()