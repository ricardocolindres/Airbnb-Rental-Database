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


    def languages_write(self, languages):
       
        for k, v in languages.items():

            list_to_pass = [ k, v['name'], v['nativeName'] ]

            print(list_to_pass)

            SQL1 = 'INSERT INTO shared.languages (iso2_language, language_name, native_name) VALUES (%s, %s, %s);'

            self.cursor.execute(SQL1, list_to_pass)
            self.conn.commit()
     

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Languages
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/LoadLanguages/languages_mockup_data.json'
    data1 = json.load(open(url))
    database.languages_write(data1)





