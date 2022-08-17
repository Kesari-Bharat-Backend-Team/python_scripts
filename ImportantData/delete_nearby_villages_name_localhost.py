
from numpy import record
import psycopg2

def connect_to_Database():

	hostname =    'localhost' # '65.1.151.184' # 
	username = 'ubuntu'
	password = 'Sm@637638' 
	database = 'Kesari_bharat'
	port = 5432

	myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )

	return myConnection
	
def myQuery(connection):


	sqlExecuter = connection.cursor()
	schema_name = 'try_admin_point' 
	file_name = 'nearby_villages_name'

	query = f"""select id, hamlet_key_value_pair, village_key_value_pair from {schema_name}.{file_name} where flag = 0"""
	sqlExecuter.execute(query)
	records = sqlExecuter.fetchall() 

	for data in records:

		id, hamlet_dict, village_dict = data 
		hamlet_dict = eval(hamlet_dict)
		village_dict = eval(village_dict)
		hamlet_id, hamlet_name = hamlet_dict

		for village_data in village_dict:

			village_gid, village_name = village_data.popitem()
			ratio = 10 
			if hamlet_name == village_name:
				
				query = f"delete from {schema_name}.villages_2019 where gid = {village_gid}"
				sqlExecuter.execute(query)
				connection.commit() 
				# print("we got same data deleteing gid = ", village_gid)
				# print('village_name = ', village_name, "hamlet_name = ", hamlet_name)
				# return 
		print(f"{file_name} , id = {id} ")		
		query = f"update {schema_name}.{file_name} set flag = 1 where id = {id}"
		sqlExecuter.execute(query)
		connection.commit() 

	print("Done!!!")

def main():
	
	myconnection = connect_to_Database() 
	print("we are calling db")
	myQuery(myconnection)
	myconnection.close() 

main() 