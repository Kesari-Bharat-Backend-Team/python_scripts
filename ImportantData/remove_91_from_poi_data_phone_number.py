import psycopg2

hostname =  'localhost' # '65.1.151.184'
username = 'ubuntu'
password = 'Sm@637638'
database = 'Kesari_bharat'
port = 5432


def myQuery(connection):

    sqlExecuter = connection.cursor()

    for i in range(501, 736):

        query = f"select phone_1, id from jan_2022_poi.poi_{i} where length(phone_1) >= 10 "
        sqlExecuter.execute(query)
        records = sqlExecuter.fetchall()
        counter = 0 

        for data in records:
            counter += 1 
            phone_1_str, id = data 
            phone_array = phone_1_str.split(",")

            for phone_number in phone_array:
                
                phone_1 = phone_number.replace(" ", "")

                if len(phone_1) > 11 and (phone_1.startswith("+91") or phone_1.startswith("91") ):
                    phone_1 = phone_1.replace("+", "")
                    phone_1 = '0' + phone_1.split(".")[0][2:]
                    # print("replace phone_number = ", phone_number, "\t updated_phone_number = ", phone_1)
                else:
                    phone_1 = phone_1.replace("+", "")
                    phone_1 = phone_1.replace("-", "")
                    phone_1 = '0' + phone_1.split(".")[0]

                    # print("<12 = ", phone_1, "\t str ->>>   ", phone_1_str)
                
                print(phone_number , " - to - ", phone_1)
                
                query = f"update jan_2022_poi.poi_{i} set phone_1 = '{phone_1}' where id = {id}"
                # print("query = ", query)
                try:
                    sqlExecuter.execute(query)
                    connection.commit() 
                except Exception as e:
                    print("error = ", str(e))
                    print("query = ", query)

            print("counter = ", counter, "poi = ", i)

        print("Done!!! poi_", i)   


def connect_to_Database():

    myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database, port = port )
    myQuery( myConnection )
    myConnection.close()

connect_to_Database()
