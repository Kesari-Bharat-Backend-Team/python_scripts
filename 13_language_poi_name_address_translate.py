
import psycopg2
import mtranslate as mp
import pandas as pd,time



import json
import sys
import random
import requests


def slackAlert(message = 'This is a test message'):

    
    url = "https://hooks.slack.com/services/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"
    # message = ("")
    title = (f"New Incoming Message :zap:")
    slack_data = {
        "username": "NotificationBot",
        "icon_emoji": ":satellite:",
        #"channel" : "#somerandomcahnnel",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
						'name': "himanshu"
						
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)




class MultiLanguageTranslate:

	def __init__(self):

		#ssd -2 

		# self.username = 'postgres'
		# self.password = '616161' 
		# self.database = 'Kesari_bharat'
		#new 

		# self.hostname = 'localhost'  
		
		self.hostname =    '3.6.253.136'  #
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.username = 'postgres'

		self.database = 'kesari_bharat'
		self.port = 5432

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 


	def doTranslation(self):

		sqlExecuter, connection = self.makeConnection()

		try:
				
			for i in range(109, 110):


				query = f"""

						select max(id) from jan_2022_poi.poi_{i}


				"""
				sqlExecuter.execute(query)
				total_records = sqlExecuter.fetchall()[0][0]

				batch_size = 100 

				for batch in range(1,  total_records, batch_size):


					query = f"""

						select name, id from 
						jan_2022_poi.poi_{i}
						where (name_translate_flag = 0 or name_translate_flag is null)
						and id between {batch} and {batch + batch_size}

					"""
					sqlExecuter.execute(query)

					poi_records = sqlExecuter.fetchall() 

					for data in poi_records:

						poi_name, poi_id = data 


						bengali = mp.translate(poi_name, "bn", "auto").replace("'", "").replace('"', "")
						gujrati = mp.translate(poi_name, "gu", "auto").replace("'", "").replace('"', "")
						hindi = mp.translate(poi_name, "hi", "auto").replace("'", "").replace('"', "")
						kannada = mp.translate(poi_name, "kn", "auto").replace("'", "").replace('"', "")
						malayalam = mp.translate(poi_name, "ml", "auto").replace("'", "").replace('"', "")
						oriya = mp.translate(poi_name, "or", "auto").replace("'", "").replace('"', "")
						punjabi = mp.translate(poi_name, "pa", "auto").replace("'", "").replace('"', "")
						sindhi = mp.translate(poi_name, "sd", "auto").replace("'", "").replace('"', "")
						tamil = mp.translate(poi_name, "ta", "auto").replace("'", "").replace('"', "")
						telegu = mp.translate(poi_name, "te", "auto").replace("'", "").replace('"', "")
						urdu = mp.translate(poi_name, "ur", "auto").replace("'", "").replace('"', "")


						translated_names = tuple([bengali, gujrati, hindi, kannada, malayalam, oriya, punjabi, sindhi, tamil, telegu, urdu, poi_id])
						print("hindi = ", hindi)
						print("english = ", poi_name)

						query = f"""

							insert into translated_poi_name.poi_{i}

							(name,bengali, gujrati, hindi, kannada, malayalam, oriya, punjabi, sindhi, tamil, telegu, urdu, poi_id)


							values ('{poi_name}', '{bengali}', '{gujrati}', '{hindi}', '{kannada}', '{malayalam}', '{oriya}', '{punjabi}', '{sindhi}', '{tamil}', '{telegu}', '{urdu}', {poi_id}) ;
						"""

						print(query)
						print("*" * 150, '\n\n')
						sqlExecuter.execute(query)

						query = f"""
							update jan_2022_poi.poi_{i}
							set name_translate_flag = 1 
							where id = {poi_id}
						"""
						sqlExecuter.execute(query)
						connection.commit()
					
					print("Done!!! ", i, "batch = ", batch)

		except Exception as e:

			slackAlert("WE got Exception in translate !!!" + str(e))
			time.sleep(5)
			self.doTranslation() 




if __name__ == '__main__':

	try:

		translate_object = MultiLanguageTranslate() 
		translate_object.doTranslation() 

	except:
		pass 
	
	finally:
		slackAlert("Translate AWS-Old  has Stopped!!!")
