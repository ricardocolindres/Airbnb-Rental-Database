# Header File: database_conn.py
# Description: Load Reward Points

import psycopg2
import json
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


    def chat_write(self, chats):

     
        SQL_HOSTS = 'SELECT user_id FROM hosts.hosts'
        self.cursor.execute(SQL_HOSTS)
        HOST_LIST = self.cursor.fetchall()
        HOST_LIST = [x[0] for x in HOST_LIST]
        for chat in chats:
            random_host = random.choice(HOST_LIST)
            chat['user_host_id'] = random_host
       
        checked_tuple = []
        corrected_chats = []
        for chat in chats:
            if chat['user_traveler_id'] == chat['user_host_id']:
                continue

            check_tuple  = (chat['user_traveler_id'], chat['user_host_id'])
            if check_tuple in checked_tuple:
                continue
            else:
                corrected_chats.append(chat)
                checked_tuple.append(check_tuple)
               

        chats = corrected_chats

        for chat in chats:

            arguments = [chat['user_traveler_id'],]
            SQL1 = 'SELECT created_date FROM users.users WHERE user_id = %s'
            self.cursor.execute(SQL1, arguments)
            user_traveler_creation_date = self.cursor.fetchone()

            arguments = [chat['user_host_id'],]
            SQL2 = 'SELECT created_date FROM users.users WHERE user_id = %s'
            self.cursor.execute(SQL2, arguments)
            user_host_creation_date  = self.cursor.fetchone()

            creation_date_dic = [user_traveler_creation_date, user_host_creation_date]
            earliest_day = max(creation_date_dic)
            earliest_day = earliest_day[0]

            current_date = datetime.strptime('2022-08-13', "%Y-%m-%d").date()
            difference_dates = ((current_date - earliest_day).days)
            dates_for_transactions = random.sample(range(1, difference_dates), 1)
            end_date = earliest_day + timedelta(days=dates_for_transactions[0])
            final_date = end_date
            final_date = datetime.strftime(final_date, "%Y-%m-%d")
            timestamp_chat = chat['timestamp']
            timestamp_chat = timestamp_chat[11:]
            final_date = final_date + ' ' + timestamp_chat
            print(final_date)

            list_to_pass = [chat['user_traveler_id'], chat['user_host_id'], chat['subject'],  chat['status'], final_date]
            print(list_to_pass)
            SQL3 = 'INSERT INTO inbox.chat_sessions (user_traveler_id, user_host_id, subject, status, time_started) VALUES (%s, %s, %s, %s, %s);'
            self.cursor.execute(SQL3, list_to_pass)
            self.conn.commit()
           

        print('Done')

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Reward Points
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/LoadChatSessions/chats_mockup_data5.json'
    data1 = json.load(open(url))
    database.chat_write(data1)





