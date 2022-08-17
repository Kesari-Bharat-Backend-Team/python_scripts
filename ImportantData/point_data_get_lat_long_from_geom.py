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
	schema_name = 'try_admin_point' 
	file_name = 'hamlet'
	query = f"select id, ST_X(geom), ST_Y(geom)  from {schema_name}.{file_name}"
	sqlExecuter.execute(query)
	records = sqlExecuter.fetchall() 


	for data in records:
		id, longitude, latitude = data
		latitude = float(latitude)
		longitude = float(longitude)

		query = f"""update {schema_name}.{file_name} set latitude = '{latitude}', longitude = '{longitude}' where id = {id}"""
		sqlExecuter.execute(query) 
		connection.commit() 
		# print(query)
		print("Updating id = ", id, latitude, longitude)

	
def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
