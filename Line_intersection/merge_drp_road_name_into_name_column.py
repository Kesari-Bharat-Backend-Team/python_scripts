import psycopg2


class LineData :

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'ubuntu'
		self.password = '$Ks123' #'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'Kesari_bharat'
		self.port = 5432

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 

	def databaseQueries(self):

		sqlExecuter, connection = self.makeConnection() 
		query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'line') ORDER BY TABLE_NAME """
		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()
		Flag = True  
		for table_name in all_tables:

			if table_name[0] == '0': continue 

			if not table_name[0].startswith('7'):
				continue 
			
			# print(table_name)

			# if int(table_name[0]) == 583:
			# 	Flag = False 
			# 	print("after the flag")
			# 	continue
			# if Flag:
			# 	continue

			print(table_name)
			# continue
			schema_name = f'line."{table_name[0]}"'
			query = f"""
					
				update {schema_name}
				set name = concat_ws(', ', name, drp_road_name)
			"""

			sqlExecuter.execute(query)
			# print(query)
			# connection.commit()
			print("Done", schema_name)
		
		connection.close()

		
	def increaseNameSize(self):
	
		sqlExecuter, connection = self.makeConnection() 

		query = f"""  SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'line') ORDER BY TABLE_NAME """
		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		for table_name in all_tables:

			if table_name[0] == '0': continue 
			schema_name = f'line."{table_name[0]}"'

			query = f"""
				ALTER TABLE {schema_name}
					ALTER COLUMN name TYPE character varying(5000)
			"""

			sqlExecuter.execute(query)
			# connection.commit() 
			print(schema_name)
			# return 


	def deletePublicPoidataOldServer(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):
#  [102, 165, 567, 574, 647, 657, 660, 673, 678]
			
			if i in  [57, 60,62,63,65, 66, 67, 68, 70, 94, 102, 109, 143, 601, 692 ] :
				continue 

			query = f"""

				drop TABLE  public.poi_old_{i}

			"""

			sqlExecuter.execute(query)
			connection.commit() 

			print(i, end = "  ")

		print("Done")




if __name__ == '__main__':

	line_object = LineData() 
	# line_object.increaseNameSize() 
	# line_object.databaseQueries() 
	line_object.deletePublicPoidataOldServer() 
