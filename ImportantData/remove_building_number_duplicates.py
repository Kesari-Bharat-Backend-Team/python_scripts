hostname = 'localhost' # '65.1.151.184' # 
username = 'ubuntu'
password = 'Sm@637638' #@'password'
database = 'Kesari_bharat'
port = 5432

import psycopg2

# query = f'''DELETE FROM jan_2022_poi.poi_{file_number} WHERE ID NOT IN ( SELECT MAX(ID) AS MaxRecordID FROM jan_2022_poi.poi_{file_number}  GROUP BY name, address, longitude, latitude );'''

# sqlExecuter.execute(query)   
# # connection.commit()


def myQuery(connection):


	sqlExecuter = connection.cursor()  

	for i in range(1, 736):
		

		table_path = f"building_no.final_id_{i}"
		# table_path = "building_no.copy_of_36"
		query = f'''
		delete from  {table_path}
		WHERE my_id NOT IN ( SELECT MAX(my_id) AS MaxRecordID 
		 from {table_path} GROUP BY routepoint, address, housenumbe, longitude, latitude );
		'''
		sqlExecuter.execute(query)
		connection.commit() 
		print("Deletion Done !!! ", table_path)








def connect_to_Database():  

	myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )
	myQuery( myConnection )
	myConnection.close()

connect_to_Database()

