# Header File: database_conn.py
# Description: Load Reward Points

from termios import TABDLY
import psycopg2
import json
import random
from datetime import datetime, timedelta


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


    def rewards_write(self, rewards):
       
        num_of_iterations = 999
        list_travelers = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350]
        selected_travelers = []
        random_num_of_entries = [1,2,3,4,5]
        total_iterations = [0,]

        #for id in wallet:
        while True:

            if len(selected_travelers) == len(list_travelers):
                print('Out from 1st')
                break
            else:
                choosen_traveler = random.choice(list_travelers)
                if choosen_traveler in selected_travelers:
                     while True:
                        choosen_traveler = random.choice(list_travelers)
                        if choosen_traveler not in selected_travelers:
                            break
                

                entries_to_travelers = random.choice(random_num_of_entries)
                arguments = [choosen_traveler,]
                SQL1 = 'SELECT created_date FROM travelers.travelers WHERE traveler_id = %s'
                self.cursor.execute(SQL1, arguments)
                returned_data = self.cursor.fetchone()
                returned_data = datetime.strftime(returned_data[0], "%Y-%m-%d")
                d1 = datetime.strptime(returned_data, "%Y-%m-%d")
                d2 = datetime.strptime('2022-08-13', "%Y-%m-%d")
                difference_dates = ((d2 - d1).days)
                dates_for_transactions = random.sample(range(1, difference_dates), entries_to_travelers)
                final_dates = []

                for day in dates_for_transactions:
                    end_date = d1 + timedelta(days=day)
                    final_dates.append(end_date)

                iterations_count = 0
                rewards_iterations = total_iterations[-1]

                if len(total_iterations) >= num_of_iterations:
                    print('Out from 2nd')
                    break

                else:
                    while True:
                        if iterations_count >= entries_to_travelers:
                            break
                        else:
                            list_to_pass = [choosen_traveler, final_dates[iterations_count], rewards[rewards_iterations]['list_numbers']]
                            print(list_to_pass)

                            SQL2 = 'INSERT INTO travelers.reward_points (traveler_id, date_transaction, points) VALUES (%s, %s, %s);'
                            self.cursor.execute(SQL2, list_to_pass)
                            self.conn.commit()
                            iterations_count += 1
                            rewards_iterations += 1
                            total_iterations.append(rewards_iterations)
            
                    selected_travelers.append(choosen_traveler)

        print('Done')

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Reward Points
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/LoadReward/reward_mockup_data.json'
    data1 = json.load(open(url))
    database.rewards_write(data1)




