from fileinput import filename
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
	for file_number in range(1, 2):
		
		file_name = f'poi_{file_number}'

		query = f"create index my_index on {schema_name}.{file_name} (name, address, latitude, longitude)"
		sqlExecuter.execute(query)
		# connection.commit() 
		print(query)
		print("Index Created Succssfully!!! ", file_name)

def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
