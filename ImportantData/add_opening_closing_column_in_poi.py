import psycopg2

# from shapely.geometry import /Point
def connect_to_Database():
	
	hostname =   'localhost' #'65.1.151.184
	username = 'ubuntu'
	password = 'Sm@637638' 
	database = 'Kesari_bharat'
	port = 5432
	myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )
	return myConnection

def myQuery(connection):

	sqlExecuter = connection.cursor()
	schema_name = 'jan_2022_poi' 

	for file_number in range(2, 736):
		
		file_name = f'poi_{file_number}'

		query = f"""ALTER TABLE IF EXISTS 
			jan_2022_poi.{file_name}
    		ADD COLUMN closing_time integer DEFAULT 1080;"""
		sqlExecuter.execute(query)
		connection.commit() 
		
		query = f"""ALTER TABLE IF EXISTS 
			jan_2022_poi.{file_name}
    		ADD COLUMN opening_time integer DEFAULT 600;"""
		sqlExecuter.execute(query)
		connection.commit() 

		query = f"""ALTER TABLE IF EXISTS 
			jan_2022_poi.{file_name}""" + """
    		ADD COLUMN  opening_days integer[] DEFAULT '{1,2,3,4,5,6,7}';"""
		sqlExecuter.execute(query)
		connection.commit() 

		print("Done!!!", file_name)


def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
