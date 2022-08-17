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

	for i in range(109, 1): 
		print(i)
		query = f"select count(*) from jan_2022_poi.poi_{i}"
		sqlExecuter.execute(query)
		total = sqlExecuter.fetchall()[0][0]
		



def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
