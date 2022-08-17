import psycopg2
import mtranslate as mp
import pandas as pd,time

# from shapely.geometry import /Point

def connect_to_Database():
	
	hostname =   'localhost' # '65.1.151.184'  #
	username = 'ubuntu'
	password = 'Sm@637638' 
	database = 'Kesari_bharat'
 
	port = 5432
	myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )
	return myConnection

def myQuery(connection):

	sqlExecuter = connection.cursor()
	schema_name = 'translated'
	file_name = 'habitation_point' 

	query = f"select habitation_name, id from {schema_name}.{file_name} where flag = 0"
	sqlExecuter.execute(query)
	records = sqlExecuter.fetchall() 

	counter = 0 
 
	for data in records:
		counter += 1 
		habitation_name, id = data
		if habitation_name is None:
			print("habitation name is None", id)	
			continue
		# print(habitation_name, id)
		try:
			bengali = mp.translate(habitation_name, "bn", "auto")
			gujrati = mp.translate(habitation_name, "gu", "auto")
			hindi = mp.translate(habitation_name, "hi", "auto")
			kannada = mp.translate(habitation_name, "kn", "auto")
			malayalam = mp.translate(habitation_name, "ml", "auto")
			oriya = mp.translate(habitation_name, "or", "auto")
			punjabi = mp.translate(habitation_name, "pa", "auto")
			sindhi = mp.translate(habitation_name, "sd", "auto")
			tamil = mp.translate(habitation_name, "ta", "auto")
			telegu = mp.translate(habitation_name, "te", "auto")
			urdu = mp.translate(habitation_name, "ur", "auto")
		except Exception as e:
			print("error aa gayi hai ", id)
			print(str(e))
			continue

		bengali = bengali.replace("'", "")
		gujrati = gujrati.replace("'", "")
		hindi = hindi.replace("'", "")
		kannada = kannada.replace("'", "")
		malayalam = malayalam.replace("'", "")
		oriya = oriya.replace("'", "")
		punjabi = punjabi.replace("'", "")
		sindhi = sindhi.replace("'", "")
		tamil = tamil.replace("'", "")
		telegu = telegu.replace("'", "")
		urdu = urdu.replace("'", "")
		
		print("id = ", id, "counter = ", counter)
		# print(bengali, gujrati, hindi, kannada, malayalam, oriya, punjabi, sindhi, tamil, telegu, urdu)

		query = f"""
			
			update {schema_name}.{file_name} 

			  set bengali = '{bengali}', 
			  gujrati = '{gujrati}',   hindi= '{hindi}',   kannada= '{kannada}',
			  malayalam = '{malayalam}',
			  oriya = '{oriya}',  punjabi = '{punjabi}',  sindhi = '{sindhi}',   tamil = '{tamil}',  telegu = '{telegu}',  urdu = '{urdu}'
			
			where id = {id}
		"""
		try:
			sqlExecuter.execute(query)
			connection.commit() 
		except Exception as e:
			print(str(e))
			print("query = ", query)
		
		query = f"update {schema_name}.{file_name} set flag = 1 where id = {id}"
		sqlExecuter.execute(query)
		connection.commit() 
		time.sleep(1)


def main():
	
	myconnection = connect_to_Database() 
	myQuery(myconnection)
	myconnection.close() 

main() 
