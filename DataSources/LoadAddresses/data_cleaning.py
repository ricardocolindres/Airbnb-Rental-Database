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


    def addresses_write(self, addresses):
       
        for address in addresses:
            SQL1 = "SELECT state_id FROM shared.cities WHERE city_name = %s and iso2_country = %s;"
            arguments = [(address['city']) , (address['country'])]
            self.cursor.execute(SQL1, arguments)
            returned_data = self.cursor.fetchone()

            if  returned_data != None: 
                returned_data = returned_data[0]
                returned_data = "".join(returned_data.split())
                print(returned_data)
                SQL1 = "SELECT state_id, city_id FROM shared.cities WHERE city_name = %s and iso2_country = %s;"
                arguments2 = [(address['city']) , (address['country'])]
                self.cursor.execute(SQL1, arguments2)
                returned_data2 = self.cursor.fetchone()   
                state_id = returned_data2[0]   
                city_id = returned_data2[1]   

                list_to_pass = [ address['address_1'], address['address_2'],
                        city_id, state_id, address['country'], address['zip_code'] ]

                print(list_to_pass)

                SQL2 = 'INSERT INTO shared.addresses (address_1, address_2, city_id, state_id, iso2_country, zip_code) VALUES (%s, %s, %s, %s, %s, %s);'

                self.cursor.execute(SQL2, list_to_pass)
                self.conn.commit()

            else:
                print('This city was not found: '+ (address['city']))

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Countries
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    #url1 = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/address_mockup_data.json'
    url2 = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/address_mockup_data.json'
    data1 = json.load(open(url2))
    database.addresses_write(data1)





