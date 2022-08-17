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
	schema_name = 'building_no' 
	file_name = 'final_id_'
	route_none_id = [] 
	hash_is_none = [] 
	exception_array = list() 
	
	for file_number in range(1, 736): 

			# query = f"select ST_X(geom), ST_Y(geom), my_id from {schema_name}.{file_name} limit 1"

			query = f"select routepoint, my_id from {schema_name}.{file_name}{str(file_number)} where (latitude is null and longitude is null)"

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:
				hash =  data[0] 
				my_id = data[1]
				if hash is None: 
					hash_is_none.append(my_id)
					continue 
				elif len(hash) < 15:
					continue
				try:

					hash = eval(hash)
				except Exception as e:
					print("We got an exception")
					exception_array.append(hash)
					exception_array.append(f"my_id = {my_id} ")
					# continue
					try:
						hash += "}"
						hash = eval(hash)
					except:
						hash += "}"
						hash = eval(hash)

				if hash and hash.get('point'):
					longitude, latitude = hash.get('point').get('coordinates')[:2]
				elif hash and hash.get('Point'):
					longitude, latitude = hash.get('Point').get('coordinates')[:2]
				else:
					print("we are unable to fetch!")
					print("we don't have hash = ", hash) 
					print("data = ", data)
					route_none_id .append(my_id)
					continue
				query = f"update {schema_name}.{file_name}{str(file_number)} set latitude='{latitude}', longitude = '{longitude}' where my_id = {my_id}" 
				sqlExecuter.execute(query)
				print(f"successfully updated! for file_no {file_number}, my_id = ", my_id)
				connection.commit() 
			print("\nDone! File number = ", file_number)

	print("Exception array = ", exception_array, "len = ", len(exception_array))
	print("\n\n")
	print("list where route is None = ", route_none_id, "\n len = ", len(route_none_id))
	print("Done!!!")
def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
