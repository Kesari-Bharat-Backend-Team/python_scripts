import psycopg2

def myQuery(connection):

	sqlExecuter = connection.cursor()
	schema_name = 'jan_2022_poi' 
	counter = 0 
	for file_number in range(99, 100):
		
		# file_name = f'poi_{file_number}'
		file_name = f'copy_jaipur'

		query = rf"select  address, id from {schema_name}.{file_name} where address like '%\\xoa0%'"
  
		sqlExecuter.execute(query)

		records = sqlExecuter.fetchall() 
  
		for data in records:
			# print(data)
			address, id = data 
			final_address = ''
			final_address = " ".join([data+ final_address for data in address.split() if data.isascii()])
			# final_address = " ".join([data+ final_address for data in address.split() if data.isalnum()])
			if final_address and final_address.isdigit() == False:

				print("final_address - ", final_address, "original ", address, id)
				query = f"update {schema_name}.{file_name} set address = '{final_address}' where id = {id}"
				sqlExecuter.execute(query)
				# connection.commit() 
    
			else:
				print("removing id = ", id, file_name, "\t address = ", address, id)
				query = f"delete from {schema_name}.{file_name} where id = {id}"
				sqlExecuter.execute(query)
				# connection.commit() 
			counter += 1 
		print("total data updeted = ", counter,  file_name)


	print("total data updeted = ", counter)
	
    				

def connect_to_Database():
	
	hostname =   'localhost' #'65.1.151.184
	username = 'ubuntu'
	password = 'Sm@637638' 
	database = 'Kesari_bharat'
	port = 5432
	myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )
	return myConnection

   
  
def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
