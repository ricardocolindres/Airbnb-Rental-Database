# Header File: database_conn.py
# Description: Load Reward Points

import psycopg2
import json
from lorem_text import lorem
import random
from datetime import datetime, timedelta, date



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


    def message_write(self):

        num_interations_per_chat = [2,3,4,5,6,7,8,9,10,11]
        #SQL1 = 'SELECT chat_id, user_traveler_id, user_host_id, time_started FROM inbox.chat_sessions;'
        SQL1 = 'SELECT user_traveler_id, user_host_id, time_started FROM inbox.chat_sessions;'
        self.cursor.execute(SQL1)
        all_chats = self.cursor.fetchall()
        for chat in all_chats:
            chosen_iterations = random.choice(num_interations_per_chat)
            iterating_list = [*range(1, chosen_iterations)]
            to_pass_tuples = []
            prepare_tuple = ('null', 'null', 'null', 'null', 'null', 'null')
            for i, num in enumerate(iterating_list):
                chosen_sender = random.choice([chat[0], chat[1]])
                words = [*range(10,30)]
                words = random.choice(words)
                message = (lorem.words(words))
                read = 'true'
                if i == (len(iterating_list)-3) and prepare_tuple[2] == chosen_sender:
                    read = random.choice(['true', 'false']) 
                elif i == (len(iterating_list)-2) and prepare_tuple[2] == chosen_sender:
                    read = random.choice(['true', 'false']) 
                elif i == (len(iterating_list)-1):
                    if prepare_tuple[4] == 'true':
                        read = random.choice(['true', 'false']) 
                    else:
                        continue
                if i == 0:
                    message_time = chat[2]
                else: 
                    last_message_time = to_pass_tuples[-1]
                    last_message_time = last_message_time[5]

                    random_minutes = random.choice([*range(2,38)])
                    given_time = last_message_time
                    new_time = given_time + timedelta(minutes=random_minutes)
                    message_time = new_time

                prepare_tuple = (chat[0], chat[1], chosen_sender, message, read, message_time)
                to_pass_tuples.append(prepare_tuple)
    
            for data in to_pass_tuples:
                list_to_pass = [data[0], data[1], data[2], data[3], data[4], data[5]]
                print(list_to_pass)
                SQL3 = 'INSERT INTO inbox.messages (user_traveler_id, user_host_id, sender, message_content, read_status, sent_at) VALUES (%s, %s, %s, %s, %s, %s);'
                self.cursor.execute(SQL3, list_to_pass)
                self.conn.commit()
           

        print('Done')

    def erase_all(self):
        number_of_entries = 160
        iterating_list = [*range(0,number_of_entries)]
        for entry in iterating_list:
            SQL4 = 'UPDATE hosts.hosts SET read_rate = 0 , response_time = %s WHERE host_id = %s;'
            host_id = ['00:00:00', entry]
            self.cursor.execute(SQL4, host_id)
            self.conn.commit()

    def add_currency(self):
        SQL5 = 'SELECT currency, currency_symbol FROM shared.countries'
        arguments = []
        self.cursor.execute(SQL5, arguments)
        currencies = self.cursor.fetchall()
        print(currencies)



if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Reward Points
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    #database.message_write()
    #database.erase_all()
    database.add_currency()





