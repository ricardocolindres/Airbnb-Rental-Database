# Header File: database_conn.py
# Description: Load Emergency Contacts

import psycopg2
import json
import random
from lorem_text import lorem


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


    def hosts_write(self, hosts):

        bad_travelers_id = []
       
        for host in hosts:
            SQL1 = "SELECT iso2_language FROM shared.languages WHERE language_name = %s;"
            arguments = [(host['main_language']),]
            self.cursor.execute(SQL1, arguments)
            returned_data = self.cursor.fetchone()
            
            if returned_data != None:
                returned_data = returned_data[0]
                host['main_language'] = returned_data
            else:
                host['main_language'] = None 
            

            if host['secondary_language'] != None: 
                SQL2 = "SELECT iso2_language FROM shared.languages WHERE language_name = %s;"
                arguments2 = [(host['secondary_language']),]
                self.cursor.execute(SQL2, arguments2)
                returned_data = self.cursor.fetchone()
                
                if returned_data != None:
                    returned_data = returned_data[0]
                    host['secondary_language'] = returned_data
                else:
                    host['secondary_language'] = None 

            SQL3 = "SELECT user_id FROM hosts.hosts WHERE user_id = %s;"
            arguments3 = [(host['traveler_id']),]
            self.cursor.execute(SQL3, arguments3)
            returned_data = self.cursor.fetchone()

            if returned_data == None:

                if  host['main_language'] != None: 

                    words = 15
                    list_to_pass = [ host['traveler_id'],host['main_language'], host['secondary_language'], host['verified'], (lorem.words(words))]

                    print(list_to_pass)
                    SQL3 = 'INSERT INTO hosts.hosts (user_id, main_language, secondary_language, verified, about) VALUES ( %s, %s, %s, %s, %s);'

                    self.cursor.execute(SQL3, list_to_pass)
                    self.conn.commit()

            else:
                bad_travelers_id.append(host['traveler_id'])
                print('pass')

           
        print('Done')
        print(bad_travelers_id)

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Emergency Contacts
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/LoadHosts/hosts_mockup_data.json'
    data1 = json.load(open(url))
    database.hosts_write(data1)





