# Header File: database_conn.py
# Description: Load Emergency Contacts

import psycopg2
import json
import random

#Install and load psycopg2 library

class DatabaseConn:

    def __init__(self, database="rentals_db", user="admin", password="rentals01", host="localhost", port="5432"):

        self.conn = psycopg2.connect(database=database,
                                     user=user,
                                     password=password,
                                     host=host,
                                     port=port)

        self.cursor = self.conn.cursor()

        self.relationships = ["Mother","Father","Brother","Sister","Elder Brother","Elder Sister","Younger Brother","Younger Sister","Son","Daughter",
                            "Grandfather (Father Of Mother)","Grandmother (Mother Of Mother)","Maternal-Grandfather","Maternal-Grandmother","Grandfather (Father Of Father)",
                            "Grandmother (Mother Of Father)","Adopted Daughter","Adopted Son","Son’s Wife (Daughter In Law)","Daughter’s Husband (Son In Law)","Niece",
                            "Nephew","Son’s Son (Grandson)","Son’s Daughter (Grand Daughter)","Daughter’s Son","Daughter’s Daughter","Husband","Wife","Fiancé Or Fiancée",
                            "Aunt","Uncle","Husband Sister (sister In Law)","Father’s Sister","Elder Sister Husband","Younger Sister Husband","Husband Elder Brother (Brother In Law)",
                            "Husband Younger Brother","Elder Brother’s Wife","Younger Brothers Wife","Wife’s Sister (Sister in Law)","Wife’s Elder Brother","Wife’s Younger Brother",
                            "Younger Sister Husband","Husband’s Elder Brother (Brother In Law)","Wife’s Brother Wife","Husband Younger Brother","Husband’s Sister’s Husband",
                            "Wife’s Sister’s Husband","Husband’s Elder Brother’s Wife","Husband’s Younger Brother’s Wife","Father’s Brother’s Son (Cousin)",
                            "Fathers Brother’s Daughter (Cousin)","Father’s Sister’s Son (Cousin)","Father’s Sister’s Daughter (Cousin)","Mother’s Brother’s Son (Cousin)",
                            "Mother’s Brother’s Daughter (Cousin)","Mother’s Sister’s Son (Cousin)","Mother’s Sister’s Daughter (Cousin)","Spouse",
                            "Spouse’s Mother (Mother In Law)","Spouse’s Father (Father In Law)","Father’s Younger Brother (Uncle)","Father’s Elder Brother (Uncle)",
                            "Father’s Younger Brother’s Wife (Aunt)","Mother’s Brother","Mother’s Younger Sister","Mother’s Younger Sister’s Husband",
                            "Mother’s Elder Sister’s Husband (Uncle)","Mother’s Elder Sister (Aunt)","Mother’s Brother Wife","Step Brother","Step Sister","Step Mother",
                            "Step Father","Step Son","Step Daughter","Mistress","Concubine / Keep Mistress","Relative","Own","Pupil","Disciple","Preceptor","Guest","Teacher",
                            "Tenant","Customer","Landlord","Friend","Lover","Girlfriend","Boyfriend","Client","Patient"]


    def commit(self):
        self.conn.commit()


    def emergency_contacts_write(self, contacts):
       
        for contact in contacts:
            SQL1 = "SELECT iso2_language FROM shared.languages WHERE language_name = %s;"
            arguments = [(contact['preferred_language']),]
            self.cursor.execute(SQL1, arguments)
            returned_data = self.cursor.fetchone()

            if  returned_data != None: 
                returned_data = returned_data[0]
                relationship = random.choice(self.relationships)
                contact_name = contact['first_name'] + ' ' + contact['last_name']

                list_to_pass = [ contact_name, relationship, returned_data, contact['email'],
                                 contact['area_code'], contact['phone'], contact['traveler_id'] ]

                print(list_to_pass)

                SQL2 = 'INSERT INTO travelers.emergency_contact(contact_name, relationship, preferred_language, email, area_code, phone, traveler_id) VALUES (%s, %s, %s, %s, %s, %s, %s);'

                self.cursor.execute(SQL2, list_to_pass)
                self.conn.commit()

            else:
                print('This language was not found: '+ (contact['preferred_language']))

if __name__ == '__main__':
  
    database = DatabaseConn()

    #Load Emergency Contacts
    #Past the JSON file's path to url1 corresponding to countries
    #Load one URL at a time. 
    url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/LoadEmergencyContact/emergency_mockup_data.json'
    data1 = json.load(open(url))
    database.emergency_contacts_write(data1)





