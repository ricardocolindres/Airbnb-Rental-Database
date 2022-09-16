# Header File: database_conn.py
# Description: Load countries, states, and cities to database

import psycopg2
import json

#Install and load psycopg2 library

class DatabaseConn:

    def __init__(self, database="rentals_db", user="admin", password="rentals01", host="localhost", port="5432"):

        self.conn = psycopg2.connect(database=database,
                                     user=user,
                                     password=password,
                                     host=host,
                                     port=port)

        self.cursor = self.conn.cursor()

    def commit(self):
        self.conn.commit()


    def travelers_write(self, travelers):
       
        for traveler in travelers:

            SQL1 = "WITH x AS (SELECT %s::varchar AS pass_hash, gen_salt('bf', 8)::varchar AS pass_salt) \
            INSERT INTO travelers.travelers(\
	        first_name, last_name, email, pass_salt, pass_hash, area_code, phone, verified_phone, birth_date, gender, government_id, active_user, created_date, last_updated, address_id)\
	        VALUES (%s, %s, %s, (SELECT x.pass_salt FROM x), (SELECT crypt(x.pass_hash, x.pass_salt) FROM x), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        
            list_to_pass = [ traveler['password'], traveler['first_name'], traveler['last_name'], traveler['email'],
                             traveler['area_code'], traveler['phone'], traveler['verified_phone'], traveler['birth_date'],
                             traveler['gender'], str(traveler['government_id']), traveler['active_user'], traveler['created_date'],
                             traveler['last_updated'], traveler['address_id']]

            print(list_to_pass)
            self.cursor.execute(SQL1, list_to_pass)
            self.conn.commit()

        print('Done')

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Travelers
    #Pass the JSON file's path to url corresponding to countries
    #Load one URL at a time. 
    url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/Load_Travelers/traveler_mock_data.json'
    data1 = json.load(open(url))
    database.travelers_write(data1)





