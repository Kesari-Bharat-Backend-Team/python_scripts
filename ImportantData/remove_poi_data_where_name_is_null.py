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
	for file_number in range(201, 736):
		
		file_name = f'poi_{file_number}'

		query = f"delete from {schema_name}.{file_name} where name is null or name = '' "
		sqlExecuter.execute(query)
		# connection.commit() 

		print("date deleted file = ", file_name)


def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
