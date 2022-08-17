import psycopg2


def get_phone_number(contacts):

	contacts = contacts.split(",")
	phone_list = []
	whatsapp_number = []
	for phone_number in contacts:
		phone_number = phone_number.replace("'", "")
		# print(phone_number)
		if "PHONE_+" in phone_number:
			phone_number = phone_number.replace("PHONE_+", "")

			try:
				phone_list.append(int(phone_number))
			except:
				pass

		elif "MOBILE_+" in phone_number:

			phone_number = phone_number.replace("MOBILE_+", "")

			try:
				whatsapp_number.append(int(phone_number))
			except:
				pass
	return phone_list, whatsapp_number


def getName(names):

	try:
		names = eval(names)

	except Exception as e:
		print(str(e))
		print("names = ", names)
		print("\n\n\n" * 10)
		names = names[3:-1]
		try:
			names = eval(names)
			if type(names) != str and type(names) != dict: return None 
			print("final_name = ", names, type(names))
		except Exception as e:
			print("exception_name = ", names)
			print(str(e))
			return

	hash = dict()
	try:

		for name_data in names:

			# print("name_data = ", name_data, "type = ", type(name_data), end = "\n")
			if type(name_data) == str:
				print("data is having string type")
				try:
					name_data = eval(name_data)
				except:
					print("we are returining ", names.get('name'))
					return names.get('name')
			name = name_data.get('name')
			languageCode = name_data.get('languageCode')
			hash[languageCode] = name

			if name_data.get("transliterations"):
				name = name_data["transliterations"][0].get('name')
				languageCode = name_data["transliterations"][0].get('languageCode')
				hash[languageCode] = name
	except Exception as e:
		print("error = ", str(e))
		return None 
	final_name = ''
	final_code = ''

	if "ENG" in hash:
		final_name = hash.get("ENG")
		final_code = "ENG"

	elif "HIX" in hash:
		final_name = hash.get("HIX")
		final_code = "HIX"

	elif "UKE" in hash:
		final_name = hash.get("UKE")
		final_code = "UKE"

	elif "UND" in hash:
		final_name = hash.get("UND")
		final_code = "UND"

	else:
		print("HASH = ", hash, names)
		final_code, final_name = hash.popitem()

	# print("name = ", final_name, end=" ")
	return final_name


def myQuery(connection1):
	print("making connection")
	sqlExecuter_postgres = connection1.cursor()

	schema_name = 'poi_here'

	for file_number in range(550, 736):

		file_name = f'{file_number}'

		# file_name = "00_jaipur_copy"

		query = f'''select names, my_id, contacts,locations, categories, postalcode, geom, district from   {schema_name}."{file_name}" where flag = 0'''

		sqlExecuter_postgres.execute(query)
		records = sqlExecuter_postgres.fetchall()

		for data in records:

			# print(file_name, end="")

			names, my_id, contacts, road, category, pincode, geom, district_name = data
			phone_arr, whatsapp_arr = get_phone_number(contacts)
			print("file_name = ", file_name, "my_id = ", my_id)
			if len(names) < 5:
				print("we are continue", names)
				continue
			name = getName(names)
			if name is None:
				print("continue is running id= ", my_id, names)
				continue
						# is we don't have have in string
						# 81name_data =  {'languageCode': 'ENG'} type =  <class 'dict'>

			category = category.replace("{", "") if category else category
			category = category.replace("}", "") if category else category
			# name = name.replace("'", "")
			phone_1 = None
			phone_2 = None
			whatsapp_number = None

			if phone_arr:
				phone_1 = phone_arr[0]
				phone_2 = str(phone_arr[1:])

			if whatsapp_arr:
				whatsapp_number = str(whatsapp_arr)

			data_source = 'ssd_2_poi_here ' + str(file_name)
			name = name.replace("'", "")
			road = road.replace("'", "") if road else road 
			data = tuple([name, phone_1, phone_2, whatsapp_number, road,
						 category, pincode, geom, district_name, data_source])
	# 		query = f'''insert into jan_2022_poi.poi_{file_number}
	# 		(name, phone_1, phone_2, whatsapp_number, road, category, pincode, geom, district_name, data_source)
	#   values {data}'''
			query = f'''insert into jan_2022_poi.poi_{file_name}
   			(name, phone_1, phone_2, whatsapp_number, road, category, pincode, geom, district_name, data_source)
	  values {data}'''

			query = query.replace("None", "null")
			sqlExecuter_postgres.execute(query)
			connection1.commit()
			query = f"""update {schema_name}."{file_name}" set flag = 1 where my_id = {my_id}"""
			sqlExecuter_postgres.execute(query)
			connection1.commit()
		print("Done!!! ", file_name)



def connect_to_Database():
	myConnection1 = psycopg2.connect(
		host='localhost', user='ubuntu', password='Sm@637638', dbname='Kesari_bharat', port=5432)
	myQuery(myConnection1)
	myConnection1.close()

connect_to_Database()
