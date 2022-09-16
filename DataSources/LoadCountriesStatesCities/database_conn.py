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

    def country_write(self, list_countries):
       
        for country in list_countries:

            try:
                phone_code = int(country['phone_code'])
            except:
                bad_code = country['phone_code']
                bad_code_country = country['name']
                print('%s is not a valid phone code for %s. Please enter one.' %(bad_code, bad_code_country) )
                phone_code = input()
                country['phone_code'] = phone_code
        
            list_to_pass = [ country['iso2'], country['iso3'], country['name'],
                        country['numeric_code'], country['phone_code'], country['currency'],
                        country['currency_symbol'], country['region'], country['latitude'],
                        country['longitude'], country['emoji'] ]

            print(list_to_pass)

            SQL2 = 'INSERT INTO shared.countries (iso2, iso3, country_name, numeric_code, phone_code, currency, currency_symbol, region, latitude, longitude, emoji) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
          
            self.cursor.execute(SQL2, list_to_pass)
            self.conn.commit()

    def state_write(self, list_states):
       
        for state in list_states:

            state_key = (state['name']) + '_' + (state['state_code'] + '_' + (state['country_code']) )
            print(state_key)
            list_to_pass = [ state_key, state['name'], state['country_code'], state['state_code'],
                        state['latitude'], state['longitude'] ]

            print(list_to_pass)

            SQL3 = 'INSERT INTO shared.states (state_id, state_name, iso2_country, state_code, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s);'
          
            self.cursor.execute(SQL3, list_to_pass)
            self.conn.commit()

    def city_write(self, list_cities):

        cities_not_added = []
       
        for city in list_cities:
            SQL5 = "SELECT state_id FROM shared.states WHERE state_id = %s;"
            state_key = [((city['state_name']) + '_' + (city['state_code']) + '_' + (city['country_code'])),]
            self.cursor.execute(SQL5, state_key)

            if self.cursor.fetchone() != None: 

                SQL9 = "SELECT city_id FROM shared.cities WHERE city_id = %s;"
                city_id = [((city['name']) + '-' +  (state_key[0])),]
                self.cursor.execute(SQL9, city_id) 
                duplicate = self.cursor.fetchone()

                if duplicate == None:

                    list_to_pass = [ city_id[0], city['name'], city['country_code'], state_key[0],
                                city['latitude'], city['longitude'], city['wikiDataId'] ]

                    print(list_to_pass)

                    SQL6 = 'INSERT INTO shared.cities (city_id, city_name, iso2_country, state_id, latitude, longitude, wiki_dataID) VALUES (%s, %s, %s, %s, %s, %s, %s);'
                
                    self.cursor.execute(SQL6, list_to_pass)
                    self.conn.commit()

                else:

                    print((duplicate[0]) + ' is duplicated and will not be added. Continued.')
                    continue
                    



            else:
                SQL7 = "SELECT state_id, state_name, iso2_country FROM shared.states WHERE state_name = %s and iso2_country = %s"
                selections = [(city['state_name']), (city['country_code'])]
                self.cursor.execute(SQL7, selections)
                state_key = self.cursor.fetchone()[0]

                if state_key != None:  

                    SQL9 = "SELECT city_id FROM shared.cities WHERE city_id = %s;"
                    city_id = [((city['name']) + '-' +  (state_key)),]
                    self.cursor.execute(SQL9, city_id) 
                    duplicate = self.cursor.fetchone()

                    if duplicate == None:

                        list_to_pass = [ city_id[0], city['name'], city['country_code'], state_key,
                                    city['latitude'], city['longitude'], city['wikiDataId'] ]

                        print(list_to_pass)

                        SQL6 = 'INSERT INTO shared.cities (city_id, city_name, iso2_country, state_id, latitude, longitude, wiki_dataID) VALUES (%s, %s, %s, %s, %s, %s, %s);'
                    
                        self.cursor.execute(SQL6, list_to_pass)
                        self.conn.commit()

                    else:

                        print((duplicate[0]) + ' is duplicated and will not be added. Continued.')
                        continue
                     
                    
                else:

                    print((city['name']) + ' ' + (city['country_code']))
                    ans = input()
                    if ans == 'y':
                        missed_tuple = ((city['name']), (city['country_code']))
                        cities_not_added.append(missed_tuple)
                        continue
                    else:
                        break
                
        print('Done')
        print(cities_not_added)

if __name__ == '__main__':
    #Pass your server credentials as arguments 
    #database="rentals_db", user="admin", password="rentals01", host="localhost", port="5432"
    database = DatabaseConn()

    #All tables must be created before running this scruipt. Table structures are included at the end. A POSTGRESQL server has been used.
    #Load countries' data

    #Load Countries
    #Past the JSON file's path to url1 corresponding to countries
    url1 = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/countries.json'
    data1 = json.load(open(url1))
    database.country_write(data1)


    #Load States
    #Past the JSON file's path to url1 corresponding to states
    url2 = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/states.json'
    data2 = json.load(open(url2))
    database.state_write(data2)


    #Load Cities
    #Past the JSON file's path to url1 corresponding to cities
    url3 = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/cities.json'
    data3 = json.load(open(url3))
    database.city_write(data3)


#! Create Database & Schema (better structure)

# CREATE DATABASE rentals_db
#     WITH 
#     OWNER = admin
#     ENCODING = 'UTF8'
#     CONNECTION LIMIT = -1;

# CREATE SCHEMA travelers
#     AUTHORIZATION admin;

# CREATE SCHEMA shared
#     AUTHORIZATION admin;

#! Create tables for Countries, States, and Cities

# CREATE TABLE shared.countries (
#     iso2 char(2) NOT NULL PRIMARY KEY,
#     iso3 char(3) NOT NULL,
#     country_name varchar(100) NOT NULL,
#     numeric_code smallint NOT NULL,
#     phone_code smallint NOT NULL,
#     currency char(3) DEFAULT NULL,
#     currency_symbol varchar(255) DEFAULT NULL,
#     region varchar(255) DEFAULT NULL,
#     latitude decimal(10,8) DEFAULT NULL,
#     longitude decimal(11,8) DEFAULT NULL,
#     emoji varchar(191)
# );

# ALTER TABLE IF EXISTS shared.countries
#     OWNER to admin;


#! Run python code to load data for countries


# CREATE TABLE shared.states (
#     state_id varchar(100) NOT NULL,
#     state_name varchar(100) NOT NULL,
#     iso2_country char(2) NOT NULL,
#     state_code char(5) NOT NULL,
#     latitude decimal(10,8) DEFAULT NULL,
#     longitude decimal(11,8) DEFAULT NULL,
#     PRIMARY KEY (state_id),
#     CONSTRAINT country_of_state FOREIGN KEY (iso2_country) REFERENCES shared.countries(iso2)
# );

# ALTER TABLE IF EXISTS shared.states
#     OWNER to admin;

#! Run python code to load data for states


# CREATE TABLE shared.cities (
#     city_id serial NOT NULL,
#     city_name varchar(100) NOT NULL,
#     iso2_country char(2) NOT NULL,
#     state_id char(100) NOT NULL,
#     latitude decimal(10,8) DEFAULT NULL,
#     longitude decimal(11,8) DEFAULT NULL,
#     wiki_dataID char(10) DEFAULT NULL,
#     PRIMARY KEY (city_id),
#     CONSTRAINT country_of_city FOREIGN KEY (iso2_country) REFERENCES shared.countries(iso2),
#     CONSTRAINT state_of_city FOREIGN KEY (state_id) REFERENCES shared.states(state_id)
# );

# ALTER TABLE IF EXISTS shared.cities
#     OWNER to admin;
    

#! Run python code to load data for cities
#! *data needs lots of cleaning
