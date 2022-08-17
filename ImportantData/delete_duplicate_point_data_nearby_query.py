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


def ratio(s1, s2):
	pass 

def myQuery(connection):

	sqlExecuter = connection.cursor()
	schema_name = 'try_admin_point' 
	file_name = 'nearby_villages_name'

	query = f"select * from {schema_name}.{file_name} where flag = 0 "
	sqlExecuter.execute(query)
	records = sqlExecuter.fetchall()

	for data in records:
		id, hamlet_key_value_pair, village_key_value_pair, flag = data
		flag = 1 

		query = f"update {schema_name}.{file_name} set flag = {flag} where id = {id}"
		sqlExecuter.execute(query)

		hamlet_key_value_pair = eval(hamlet_key_value_pair)
		village_key_value_pair = eval(village_key_value_pair)

		hamlet_id, hamlet_name = hamlet_key_value_pair

		for data in village_key_value_pair:

			village_id, village_name = list(data.items())[0]

			if hamlet_name == village_name:
				print("both names are same ", hamlet_id, village_id)
				print(hamlet_name, village_name)
				print("deleting village id \n\n\n")
				
				query = f"select * from {schema_name}.villages_2019 where gid = {village_id} "
				sqlExecuter.execute(query)
				query_data = sqlExecuter.fetchall() 
				# print(query_data)
			elif ratio(hamlet_name, village_name) >= 80:
				
				my_set = set() 
				query = f"select * "



		print("id = ", id, "Done !!!")


def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
