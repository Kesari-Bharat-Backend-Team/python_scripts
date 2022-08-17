import psycopg2
from math import radians, cos, sin, asin, sqrt

class LineData:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'ubuntu'
		self.password = '$Ks123'
		self.database = 'Kesari_bharat'
		self.port = 5432

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 
	
	def distance(self, lat1, lat2, lon1, lon2):
	
		# The math module contains a function named
		# radians which converts from degrees to radians.
		lon1 = radians(lon1)
		lon2 = radians(lon2)
		lat1 = radians(lat1)
		lat2 = radians(lat2)
		
		# Haversine formula
		dlon = lon2 - lon1
		dlat = lat2 - lat1
		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2

		try:

			c = 2 * asin(sqrt(a))

		except Exception as e: 

			print("We got an Exception is distance function ", str(e))
			print("returning 100 for now!!")
			return 100
			
		# Radius of earth in kilometers. Use 3956 for miles
		r = 6371
		
		# calculate the result
		return round(c * r, 5)


	def getLatLongList(self, schema_name = 'edited_110', my_id = 141550 ):

		sqlExecuter, connection = self.makeConnection()

		query = f"""
		   
			SELECT ST_AsText ( ST_Transform( (ST_Dumppoints (geom)).geom, 4326) )
			FROM {schema_name}
			where my_id = {my_id}

		"""

		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()
		
		temp_array = [] 
		for data in records:
			lon, lat =  data[0][6:-4].split(" ")
			temp_array.append([float(lon), float(lat)])

		return temp_array

	def getLatLonDifference(self, arr1, arr2):

		answer_array = []

		for data in arr1:
		
			lon1, lat1 = data #73.5140221957832, 26.0940516031
			minimum = 100000
			for arr2_data in arr2:
				lon2, lat2 = arr2_data #73.5297678032012, 26.1286388987
				answer = self.distance(lat1, lat2, lon1, lon2)
				minimum = min(answer, minimum)
			answer_array.append(minimum)

		answer_array.sort() 

		for data in answer_array[:4]:
			if data > 0.009:
				return False 
		print(answer_array)
		return True 
		# print(sorted(answer_array)[:5])

	def dbQueries(self):

		sqlExecuter, connection = self.makeConnection() 

		# query = f""" 
			
		# 	SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
		# 	table_name ilike 'edi%' 
			

		# """
		# sqlExecuter.execute(query)
		# all_tables = sqlExecuter.fetchall()

		# for table_name in all_tables:
			
		# 	table_name = table_name[0]
			
		# 	query = f"""
		# 			alter table if exists edited_line.{table_name}
		# 			add column if not exists my_id serial;
		# 	"""
		# 	sqlExecuter.execute(query)
		# 	connection.commit()
		# 	print(query)


		# return

		
		all_tables_1 = ['edited_642', 'edited_698', 'edited_392', 'edited_100', 'edited_225', 'edited_467', 'edited_48', 'edited_492', 'edited_497', 'edited_533', 'edited_539', 'edited_546', 'edited_548', 'edited_550', 'edited_551', 'edited_552', 'edited_590', 'edited_591', 'edited_594', 'edited_606', 'edited_615', 'edited_660', 'edited_722', 'edited_80', 'edited_81', 'edited_87', 'edited_109', 'edited_110', 'edited_218', 'edited_232', 'edited_241']
		

		all_tables_2 = ['edited_73', 'edited_54', 'edited_692', 'edited_530', 'edited_715', 'edited_96', 'edited_405', 'edited_582', 'edited_614', 'edited_97', 'edited_380', 'edited_630', 'edited_120', 'edited_72', 'edited_198', 'edited_588', 'edited_117', 'edited_508', 'edited_111', 'edited_65', 'edited_385', 'edited_60', 'edited_40', 'edited_471', 'edited_177', 'edited_488', 'edited_599', 'edited_215', 'edited_489', 'edited_104', 'edited_557', 'edited_129', 'edited_212', 'edited_498', 'edited_184', 'edited_553', 'edited_462', 'edited_520', 'edited_115']
		
		all_tables_3 = ['edited_414', 'edited_501', 'edited_174', 'edited_176', 'edited_38', 'edited_221', 'edited_547', 'edited_219', 'edited_36', 'edited_160', 'edited_178', 'edited_43', 'edited_491', 'edited_183', 'edited_496', 'edited_394', 'edited_502', 'edited_459', 'edited_481', 'edited_513', 'edited_186', 'edited_153', 'edited_521', 'edited_246', 'edited_522', 'edited_523', 'edited_581', 'edited_447', 'edited_454', 'edited_190', 'edited_438', 'edited_585', 'edited_195', 'edited_592', 'edited_205', 'edited_242', 'edited_543', 'edited_231', 'edited_693', 'edited_397', 'edited_536', 'edited_122', 'edited_17', 'edited_197', 'edited_544', 'edited_577', 'edited_78']
		
		all_tables_4 = ['edited_243', 'edited_74', 'edited_216', 'edited_220', 'edited_227', 'edited_244', 'edited_133', 'edited_91', 'edited_136', 'edited_534', 'edited_537', 'edited_542', 'edited_125', 'edited_529', 'edited_113', 'edited_85', 'edited_482', 'edited_680', 'edited_86', 'edited_674', 'edited_675', 'edited_668', 'edited_672', 'edited_653', 'edited_654', 'edited_666', 'edited_652', 'edited_648', 'edited_650', 'edited_583', 'edited_644']

		all_tables_5 = [ 'edited_240', 'edited_222', 'edited_404', 'edited_233', 'edited_238', 'edited_106', 'edited_52', 'edited_247', 'edited_118', 'edited_402', 'edited_44', 'edited_37', 'edited_213', 'edited_369', 'edited_57', 'edited_108', 'edited_135', 'edited_55', 'edited_105', 'edited_107', 'edited_239', 'edited_532', 'edited_535', 'edited_202', 'edited_593', 'edited_203', 'edited_567', 'edited_586', 'edited_94', 'edited_139']

		all_tables_6 = [ 'edited_230', 'edited_396', 'edited_51', 'edited_53', 'edited_411', 'edited_555', 'edited_228', 'edited_474', 'edited_158', 'edited_42', 'edited_476', 'edited_673', 'edited_82', 'edited_147', 'edited_169', 'edited_140', 'edited_191', 'edited_149', 'edited_477', 'edited_141', 'edited_485', 'edited_229', 'edited_83', 'edited_185', 'edited_378', 'edited_165', 'edited_214', 'edited_50', 'edited_84', 'edited_166', 'edited_172', 'edited_415', 'edited_46', 'edited_47', 'edited_170', 'edited_41', 'edited_224', 'edited_173', 'edited_39', 'edited_478']
		
		
		
		all_tables_7 = ['edited_102', 'edited_531', 'edited_194', 'edited_528', 'edited_200', 'edited_527', 'edited_7', 'edited_423', 'edited_426', 'edited_573', 'edited_121', 'edited_119', 'edited_566', 'edited_556', 'edited_564', 'edited_143', 'edited_541', 'edited_144', 'edited_505', 'edited_540', 'edited_145', 'edited_146', 'edited_509', 'edited_525', 'edited_538', 'edited_148', 'edited_151', 'edited_435', 'edited_237', 'edited_248', 'edited_154', 'edited_236', 'edited_206', 'edited_79', 'edited_207', 'edited_235', 'edited_155', 'edited_234', 'edited_393', 'edited_209', 'edited_201', 'edited_609', 'edited_77', 'edited_211', 'edited_156', 'edited_473', 'edited_217']
		
		all_tables_8 = [ 'edited_401', 'edited_549', 'edited_422', 'edited_425', 'edited_428', 'edited_436', 'edited_526', 'edited_418', 'edited_440', 'edited_421', 'edited_427', 'edited_416', 'edited_665', 'edited_127', 'edited_663', 'edited_689', 'edited_88', 'edited_89', 'edited_670', 'edited_92', 'edited_651', 'edited_688', 'edited_75', 'edited_249', 'edited_223']
		
		all_tables_9 = [ 'edited_646', 'edited_580', 'edited_575', 'edited_578', 'edited_569', 'edited_572', 'edited_134', 'edited_600', 'edited_132', 'edited_167', 'edited_608', 'edited_112', 'edited_554', 'edited_632', 'edited_126', 'edited_702', 'edited_114', 'edited_475', 'edited_124', 'edited_128', 'edited_90', 'edited_655', 'edited_142', 'edited_71', 'edited_687']


		all_tables_10 = [ 'edited_584', 'edited_49', 'edited_601', 'edited_483', 'edited_545', 'edited_56', 'edited_103', 'edited_603', 'edited_123', 'edited_45', 'edited_116', 'edited_130', 'edited_382', 'edited_384', 'edited_131', 'edited_95', 'edited_443', 'edited_579', 'edited_596', 'edited_611']
		
		



		table_name = input(("Enter the table Name = "))

		hashmap = {

			"1": all_tables_1,
			"2": all_tables_2,
			"3": all_tables_3,
			"4": all_tables_4,
			"5": all_tables_5,
			"6": all_tables_6,
			"7": all_tables_7,
			"8": all_tables_8,
			"9": all_tables_9,
			"10" : all_tables_10

		}

		table_array_name = f'{table_name} is my table name '
		
		for i in hashmap[table_name]: #[::-1]: #revesring the array for simultanious execution 

			# file_name = i[0]
			file_name = i 
			

			schema_name = f'edited_line.{file_name}'
			
			query = f"""

				select my_id, path, geom
				FROM {schema_name}
				where path ilike 'facebook%' and checkduplicateflag = 0 and duplicate_flag = 0
				
			"""

			print("query = ", query)
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
				
				my_id_facebook, path, geom_facebook = data 
				if geom_facebook is None or len(geom_facebook) < 5 : continue  #ST_DWithin('None', geom, 1000) Edge Case

				#update Flag 
				query = f"""

						update  {schema_name}
						set checkduplicateflag = 1
						where my_id = {my_id_facebook}
						
				"""
				sqlExecuter.execute(query)
				connection.commit() 
				print(query)

				query = f"""

					select my_id, path from {schema_name}
						where path ilike 'os%' and 
					ST_DWithin('{geom_facebook}', geom, 1000) 

				"""
				print(query)

				sqlExecuter.execute(query)
				osm_records = sqlExecuter.fetchall() 

				arr1 = self.getLatLongList(schema_name = schema_name, my_id = my_id_facebook)
				
				for osm_data in osm_records:
				
					my_id_osm, path = osm_data
					print(my_id_osm, table_array_name)
					arr2 =  self.getLatLongList(schema_name = schema_name, my_id = my_id_osm )
					is_duplicate = self.getLatLonDifference(arr1, arr2) 

					if is_duplicate:
						print("facebook = ", my_id_facebook,  " osm = ",my_id_osm) 

						query = f"""

							update {schema_name}
							set duplicate_flag = 1
							where my_id = {my_id_facebook}
						"""
						sqlExecuter.execute(query)
						connection.commit() 
						print("We got duplicate ", query)

						break
					else:
						print(".", end = "")

		print("\n\nDone!!!! Ho gaya.")


	def dbQueriesForLineData(self):

		sqlExecuter, connection = self.makeConnection() 

		schema_name = "line"
		# query = f""" 
			
		# 	SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = '{schema_name}') 
		# """
		# sqlExecuter.execute(query)
		# all_tables = sqlExecuter.fetchall()

		
		start = int(input("Enter start = "))
		end = int(input("Enter end = "))
		
		for i in range(start, end):


			# file_name = i[0]

			file_name = i 
			query = f"""

					SELECT EXISTS (
					SELECT FROM 
						information_schema.tables 
					WHERE 
						table_schema  = 'line' AND 
						table_name   = '{i}'
					);
			"""
			sqlExecuter.execute(query)
			result = sqlExecuter.fetchall()[0][0] 

			if not result:
				continue

			query = f"""

				alter table if exists {schema_name}."{file_name}"
				add column if not exists my_id serial;

			"""

			sqlExecuter.execute(query)
			connection.commit() 


			query = f"""

				select my_id, path, geom
				FROM {schema_name}."{file_name}"
				where path ilike 'facebook%' and checkduplicateflag = 0 and duplicate_flag = 0
				
			"""

			print("query = ", query)
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
				
				my_id_facebook, path, geom_facebook = data 
				if geom_facebook is None or len(geom_facebook) < 5 : continue  #ST_DWithin('None', geom, 1000) Edge Case

				#update Flag 
				query = f"""

						update  {schema_name}."{file_name}"
						set checkduplicateflag = 1
						where my_id = {my_id_facebook}
						
				"""
				sqlExecuter.execute(query)
				connection.commit() 
				print(query)

				query = f"""

					select my_id, path from {schema_name}."{file_name}"
						where path ilike 'os%' and 
					ST_DWithin('{geom_facebook}', geom, 1000) 

				"""
				print(query)

				sqlExecuter.execute(query)
				osm_records = sqlExecuter.fetchall() 

				arr1 = self.getLatLongList(schema_name = f'{schema_name}."{file_name}"', my_id = my_id_facebook)
				
				for osm_data in osm_records:
				
					my_id_osm, path = osm_data
					print(my_id_osm, "file_name = ", file_name)
					arr2 =  self.getLatLongList(schema_name = f'{schema_name}."{file_name}"', my_id = my_id_osm )
					is_duplicate = self.getLatLonDifference(arr1, arr2) 

					if is_duplicate:
						print("facebook = ", my_id_facebook,  " osm = ",my_id_osm) 

						query = f"""

							update {schema_name}."{file_name}"
							set duplicate_flag = 1
							where my_id = {my_id_facebook}
						"""
						sqlExecuter.execute(query)
						connection.commit() 
						print("We got duplicate ", query)

						break
					else:
						print(".", end = "")

		print("\n\nDone!!!! Ho gaya.")


	def deleteFaceBookID(self):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""
		select my_id, id from edited_line.duplicate_edited_110_new 
		/*limit 5 */ 

		"""

		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 

		for data in records:

			my_id, id = data 
			print(my_id)

			query = f"""
					delete  from edited_line.edited_110
					where my_id = {my_id}
			"""
			sqlExecuter.execute(query)
			# connection.commit()
			# print(query)
		
	def addColumnDuplicateFlag(self):

		sqlExecuter, connection = self.makeConnection() 

		schema_name = "line"
		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = '{schema_name}')
			

		"""
		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		for i in all_tables:

			table_name = i[0]

			query = f"""

				alter table {schema_name}."{table_name}"
				add column if not exists duplicate_flag integer default 0,
				add column if not exists checkduplicateflag integer default 0;

			"""

			sqlExecuter.execute(query)
			connection.commit()
			print(query)
		
			
		print("Done!!!")

	def checkDuplicateFlag(self):

		sqlExecuter, connection = self.makeConnection() 

		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
			table_name ilike 'edi%' 
			

		"""
		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		for i in all_tables:

			file_name = i[0]
			
			# print("file_name = ", file_name)

			query = f"""

				select count(*) from 
				edited_line.{file_name}
				where checkduplicateflag = 1

			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
				
				temp = records[0][0]
				if temp:
					print(file_name, temp)

	def checkColumn(self):

		
		sqlExecuter, connection = self.makeConnection() 


		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
			table_name ilike 'edi%' 
			

		"""
		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		for i in all_tables:

			file_name = i[0]
			# print("file_name = ", file_name)

			query = f"""

				SELECT table_name 
				FROM information_schema.columns 
				WHERE table_name='{file_name}' and column_name = 'duplicate_flag';

			"""

			sqlExecuter.execute(query)

			records = sqlExecuter.fetchall()
			if records:

				# print(records[0])
				pass 
			
			else:
			
				print(file_name)


	def printTables(self):

		
		sqlExecuter, connection = self.makeConnection() 


		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
			table_name ilike 'edi%' 
			

		"""
		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		table_names = [] 

		for i in all_tables:

			file_name = i[0]
			table_names.append(file_name)
		
		print(table_names)


	def countandDeleteDuplicateRoads(self):

		sqlExecuter, connection = self.makeConnection() 


		start = int(input("Enter starting table = "))
		end = int(input("Enter ending table = "))


		total = 1

		not_visited_tables = [] 


		for i in range(start, end):

			print("i = ", i)
			
			if i in [392, 439, 593, 679, 691, 262, 263, 266, 453, 474, 475, 698, 699, 700]: continue 
		
			query = f"""

					SELECT EXISTS (
					SELECT FROM 
						information_schema.tables 
					WHERE 
						table_schema  = 'line' AND 
						table_name   = '{i}'
					);
			"""
			sqlExecuter.execute(query)
			result = sqlExecuter.fetchall()[0][0] 

			if result:

				table_path = f"""line."{i}" """

			else:

				table_path = f""" edited_line . edited_{i} """



			query = f"""

				delete  from {table_path}
				where duplicate_flag = 1;
			
			"""

			sqlExecuter.execute(query)
			connection.commit() 

			print(query)
		# 	records = sqlExecuter.fetchall()[0][0]

		# 	if not records:
		# 		print(table_path, "\t\t", records)
		# 		not_visited_tables.append(i)
			
		# print(not_visited_tables)

				# print("we got ")
			# total += records

		# print("total = ", total)



line_object = LineData() 
line_object.countandDeleteDuplicateRoads() 

# line_object.printTables() 

# line_object.dbQueries() 
# line_object.dbQueriesForLineData() 
# line_object.addColumnDuplicateFlag() 


	
# line_object.addColumnDuplicateFlag() 
# line_object.deleteFaceBookID() 
# line_object.getLatLonDifference() 

