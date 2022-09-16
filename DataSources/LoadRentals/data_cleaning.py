# Header File: database_conn.py
# Description: Load Reward Points

from faker import Faker
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

    def add_rentals(self, rentals):

        address_id_count = 401
        for rental in rentals:   
            #prepare address id
            rental['address_id'] = address_id_count 
            #prepare title name
            SQL1 = 'SELECT cities.city_name FROM shared.cities AS cities \
            JOIN shared.addresses AS addresses ON addresses.city_id = cities.city_id \
            WHERE addresses.address_id = %s;'
            query_arguments = [address_id_count,]
            self.cursor.execute(SQL1, query_arguments)
            address_city_name = self.cursor.fetchone()
            rental_name = rental['title'] + ' Home at ' + address_city_name[0]
            rental['title'] = rental_name
            #prepare description
            words = [*range(10,30)]
            words = random.choice(words)
            message = (lorem.words(words))
            rental['description'] = message
            #prepare host
            SQL2 = 'SELECT host_id FROM hosts.hosts;'
            self.cursor.execute(SQL2)
            all_hosts = [x[0] for x in (self.cursor.fetchall())]
            rental['host_id'] = random.choice(all_hosts)
            #prepare cleaning fee
            if rental['cleaning_fee'] == True:
                discount = (random.choice([*range(80,100)]))/100
                rental['cleaning_fee'] = round((rental['price'] * discount), 2) 
            else:
                rental['cleaning_fee'] = 0.00
            #prepare extra guest fee
            if rental['extra_guest_fee'] == True:
                avg_guest_price = rental['price'] / rental['allowed_guests']
                discount = (random.choice([*range(60,80)]))/100
                avg_guest_price = round((avg_guest_price * discount), 2) 
                if (rental['price'] - avg_guest_price) > (rental['price'] * 0.5):
                    rental['extra_guest_fee'] = round((rental['price'] * 0.20), 2)
                else:
                    rental['extra_guest_fee'] = avg_guest_price

            else:
                rental['extra_guest_fee'] = 0.00
            #prepare pet fee
            if rental['pet_fee'] == True:
                discount = (random.choice([*range(5,20)]))/100
                rental['pet_fee'] = round((rental['price'] * discount), 2) 
            else:
                rental['pet_fee'] = 0.00
            # Prepare Min stay
            if rental['min_stay'] == None:
                rental['min_stay'] = '1 days'
            else:
                if rental['max_stay'] != None:
                    if rental['min_stay'] >= rental['max_stay']:
                        rental['min_stay'] = '1 days'
                    else:
                        rental['min_stay'] = str(rental['min_stay']) + ' days'
                else:
                    rental['min_stay'] = str(rental['min_stay']) + ' days'

            # Prepare Max stay
            if rental['max_stay'] == None:
                rental['max_stay'] = '5 years'
            else:
                rental['max_stay'] = str(rental['max_stay']) + ' days'

            address_id_count += 1
            # Check-In and Check-Out Max stay
            check_out_options = ['07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00']
            check_in_options = ['13:00:00', '14:00:00', '15:00:00', '16:00:00']
            rental['check_in_window'] = random.choice(check_in_options)
            rental['check_out_window'] = random.choice(check_out_options)
            # Prepare Cancelation policy
            cancellation_policy = ['Flexible', 'Flexible or Non-refundable', 'Moderate', 'Moderate or Non-refundable', 'Firm', 'Firm or Non-refundable', 'Strict', 'Strict or Non-refundable']
            rental['cancellation_policy'] = random.choice(cancellation_policy)
            # Prepare Guest_Id and Isntant Booking
            if rental['guest_id_required'] == None:
                rental['guest_id_required'] = True
            if rental['instant_booking'] == None:
                rental['instant_booking'] = True
            if rental['guest_id_required'] == False:
                rental['instant_booking'] = False
            #Prepare Property Type
            property_ids = [*range(1,9)] + [*range(1,62)] + [*range(1,9)]  + [*range(1,9)] + [*range(1,9)] + [*range(1,9)]
            rental['property_id'] = random.choice(property_ids)
            #Prepare Creation_date
            SQL4 = 'SELECT users.created_date FROM users.users AS users \
                    JOIN hosts.hosts AS hosts ON users.user_id = hosts.user_id \
                    WHERE hosts.host_id = %s'
            query_arguments2 = [(rental['host_id']),]
            self.cursor.execute(SQL4, query_arguments2)
            host_creation_date = (self.cursor.fetchone())[0]
            current_date = datetime.strptime('2022-08-30', "%Y-%m-%d").date()
            difference_dates = ((current_date - host_creation_date).days)
            days_to_add = random.choice(range(1, difference_dates))
            rental_creation_date = host_creation_date + timedelta(days=days_to_add)
            rental_creation_date = datetime.strftime(rental_creation_date, "%Y-%m-%d")
            rental['creation_date'] = rental_creation_date
            

            
        for x in rentals:
            SQL3 = 'INSERT INTO rentals.rentals \
                    (host_id, title, description, allowed_guests, address_id, price, listing_currency, \
                    weekly_discount, monthly_discount, early_bird_discount, last_minute_discount, cleaning_fee, extra_guest_fee, \
                    pet_fee, min_stay, max_stay, check_in_window, check_out_window, cancellation_policy, guest_id_required, \
                    instant_booking, property_id, listing_type, creation_date) \
	                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
            list_to_pass = [x['host_id'], x['title'], x['description'], x['allowed_guests'], x['address_id'], x['price'], 'USD',
                    x['weekly_discount'], x['monthly_discount'], x['early_bird_discount'], x['last_minute_discount'], x['cleaning_fee'], x['extra_guest_fee'],
                    x['pet_fee'], x['min_stay'], x['max_stay'], x['check_in_window'], x['check_out_window'], x['cancellation_policy'], x['guest_id_required'], 
                    x['instant_booking'], x['property_id'], x['listing_type'], x['creation_date']]
            self.cursor.execute(SQL3, list_to_pass)
            self.conn.commit()
            print(x['title'])

        print('Done!')

    def amenities_rentals(self):
        SQL1 = 'SELECT amenity FROM rentals.amenities;'
        self.cursor.execute(SQL1)
        amenitites = [x[0] for x in (self.cursor.fetchall())]
        SQL2 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL2)
        rental_id = [x[0] for x in (self.cursor.fetchall())]
        for x in rental_id:
            amenitites_chosen = []
            numbers_of_amenities = random.choice([*range(4,16)])
            for num in [*range(1,(numbers_of_amenities + 1))]:
                select_amenity = random.choice(amenitites)
                amenitites_chosen.append(select_amenity)
            amenitites_chosen = list(set(amenitites_chosen))
            for a in amenitites_chosen:
                SQL3 = 'INSERT INTO rentals.rentals_and_amenities( \
                        rental_id, amenity) VALUES (%s, %s);'
                list_to_pass = [x, a]
                self.cursor.execute(SQL3, list_to_pass)
                self.conn.commit()

    def spaces_rentals(self):
        SQL1 = 'SELECT spaces FROM rentals.spaces;'
        self.cursor.execute(SQL1)
        spaces = [x[0] for x in (self.cursor.fetchall())]
        SQL2 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL2)
        rental_id = [x[0] for x in (self.cursor.fetchall())]
        check_repeat = []
        for x in rental_id:
            spaces_chosen = []
            numbers_of_spaces = random.choice([*range(4,11)])
            for num in [*range(1,numbers_of_spaces)]:
                select_spaces = random.choice(spaces)
                if select_spaces in check_repeat:
                    continue
                shared_space = random.choice([False, False, False, False, True,])
                if shared_space == True:
                    SQL3 = 'SELECT sharing_groups FROM rentals.share_options;'
                    self.cursor.execute(SQL3)
                    sharing_groups = [x[0] for x in (self.cursor.fetchall())]
                    shared_with = random.choice(sharing_groups)
                else:
                    shared_with = None

                quanitity_spaces = random.choice([1,1,1,1,1,2,3])
                check_repeat.append(select_spaces)
                spaces_chosen.append((select_spaces, quanitity_spaces, shared_space, shared_with))
                
            spaces_chosen = list(set(spaces_chosen))
            for a in spaces_chosen:     
                SQL4 = 'INSERT INTO rentals.rentals_and_spaces( \
                        rental_id, spaces, quantity, shared_spaces, shared_with) VALUES (%s, %s, %s, %s, %s);'
                list_to_pass = [x, a[0], a[1], a[2], a[3]]
                self.cursor.execute(SQL4, list_to_pass)
                self.conn.commit()
                print(a)

            check_repeat = []
        print('done')

    def calendar_availability(self):
     
        preparation_time = ['0 days', '1 days', '2 days']        
        advance_notice = ['1 days', '2 days', '3 days', '7 days']
        availability_window = ['3 months', '6 months', '9 months', 
                                '1 years', '5 years', '1 years', '5 years',
                                '1 years', '5 years', '1 years', '5 years']
        same_day_booking_max_time = ['07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00', '12:00:00',
                                    '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00',
                                    '12:00:00', '13:00:00', '14:00:00', '12:00:00', '13:00:00', '14:00:00',
                                    '12:00:00', '13:00:00', '14:00:00']

        SQL1 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL1)
        rental_id = [x[0] for x in (self.cursor.fetchall())]

        for x in rental_id:

            SQL2 = 'SELECT instant_booking FROM rentals.rentals WHERE rental_id = %s;'
            passing_id = [x, ]
            self.cursor.execute(SQL2, passing_id)
            instant_booking = self.cursor.fetchone()[0]
            
            if instant_booking == True:
                (print('im if'))
                chosen_availability_window = random.choice(availability_window)
                chosen_preparation_time = '0 days'
                chosen_advance_notice = '0 days'
                choosen_same_day_booking_max_time = random.choice(same_day_booking_max_time)
                print(choosen_same_day_booking_max_time)
                list_to_pass = [x, chosen_availability_window, chosen_preparation_time, chosen_advance_notice, choosen_same_day_booking_max_time]
                SQL3 = 'INSERT INTO rentals.calendar_availability( \
                        rental_id, availability_window, preparation_time, advance_notice, same_day_booking_max_time) \
                        VALUES (%s, %s, %s, %s, %s);'
                self.cursor.execute(SQL3, list_to_pass)
                self.conn.commit()
                print(list_to_pass)
            else:
                print('im else')
                chosen_availability_window = random.choice(availability_window)
                chosen_preparation_time = random.choice(preparation_time)
                chosen_advance_notice = random.choice(advance_notice)
                choosen_same_day_booking_max_time = None
                list_to_pass = [x, chosen_availability_window, chosen_preparation_time, chosen_advance_notice, choosen_same_day_booking_max_time]
                SQL3 = 'INSERT INTO rentals.calendar_availability( \
                        rental_id, availability_window, preparation_time, advance_notice, same_day_booking_max_time) \
                        VALUES (%s, %s, %s, %s, %s);'
                self.cursor.execute(SQL3, list_to_pass)
                self.conn.commit()
                print(list_to_pass)


        print('done')

    def add_rules(self):
        rules = ['Suitable for children (2-12 years)', 'Suitable for infants (under 2 years)', 
                'Pets allowed', 'Smoking allowed', 'Events allowed', 'Suitable for infants (under 2 years)',
                'Events allowed', 'Suitable for infants (under 2 years)', 'Events allowed', 'Pets allowed',]
        SQL1 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL1)
        rental_id = [x[0] for x in (self.cursor.fetchall())]
        check_repeat = []
        for x in rental_id:
            rules_chosen = []
            numbers_of_rules = random.choice([*range(1,7)])
            for num in [*range(1,numbers_of_rules)]:
                selected_rule = random.choice(rules)
                if selected_rule in check_repeat:
                    continue
                else:
                    rules_chosen.append(selected_rule)
                    check_repeat.append(selected_rule)

            for a in rules_chosen:     
                SQL4 = 'INSERT INTO rentals.rentals_and_rules( \
                        rental_id, rule_title) VALUES (%s, %s);'
                list_to_pass = [x, a]
                self.cursor.execute(SQL4, list_to_pass)
                self.conn.commit()
                print(list_to_pass)
            check_repeat = []

        print('done')

    def add_custom_pricing(self):
        SQL1 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL1)
        rental_id = [x[0] for x in (self.cursor.fetchall())]
        random_date_generator = Faker()

        for x in rental_id:
            SQL2 = 'SELECT creation_date FROM rentals.rentals WHERE rental_id = %s;'
            self.cursor.execute(SQL2, [x,])
            rental_creation_date = self.cursor.fetchone()[0]

            SQL3 = 'SELECT availability_window FROM rentals.calendar_availability WHERE rental_id = %s;'
            self.cursor.execute(SQL3, [x,])
            availability_window = self.cursor.fetchone()[0].days

            SQL2 = 'SELECT price FROM rentals.rentals WHERE rental_id = %s;'
            self.cursor.execute(SQL2, [x,])
            price = self.cursor.fetchone()[0]
            price = float(price.replace('$', ''))

            max_reservation_possible = datetime.today() + timedelta(days=availability_window)

            number_of_custom_prices = random.choice([*range(1,11)])
            possible_intervals = [1,2,3,4,5,6,7,2,7,3,1,7,3,1,1]
            pre_assembled_data = []
            for pz in [*range(1,number_of_custom_prices)]:
                chosen_interval = random.choice(possible_intervals)
                discount = (random.choice([*range(50,90)]))/100
                new_price = round((price * discount),2)
                new_date = random_date_generator.date_between(start_date= rental_creation_date, end_date= max_reservation_possible)
                prepared_tuple = (new_date, chosen_interval, new_price)
                pre_assembled_data.append(prepared_tuple)

            assembled_data = []
            for tpl in pre_assembled_data:
                good_date = True
                for tpl2 in pre_assembled_data:
                    if tpl == tpl2:
                        continue
                    else:
                        custom_price_final_date = tpl[0] + timedelta(days=tpl[1])
                        if tpl2[0] <= custom_price_final_date <= (tpl2[0] + timedelta(days=tpl2[1])):
                            print('Bad Date')
                            good_date = False
                        
                if good_date == True:
                    assembled_data.append(tpl)
                        
            for d in assembled_data:     
                SQL4 = 'INSERT INTO rentals.custom_pricing( \
	                    rental_id, scheduled_date, discounted_price, continuous_intervals) \
	                    VALUES (%s, %s, %s, %s);'
                list_to_pass = [x, d[0], d[2], (str(d[1]) + ' days')]
                self.cursor.execute(SQL4, list_to_pass)
                self.conn.commit()
                print(list_to_pass)

        print('done')

    def reservations(self):
        SQL1 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL1)
        rental_id = [x[0] for x in (self.cursor.fetchall())]

        SQL2 = 'SELECT user_id FROM users.users;'
        self.cursor.execute(SQL2)
        user_ids = [x[0] for x in (self.cursor.fetchall())]
        random_date_generator = Faker()

        for x in rental_id:
            SQL3 = 'SELECT hosts.user_id FROM hosts.hosts \
                    JOIN rentals.rentals ON rentals.host_id = hosts.host_id \
                    WHERE rentals.rental_id = %s;'
            self.cursor.execute(SQL3, [x,])
            rental_host_user_id = self.cursor.fetchone()[0]

            SQL4 = 'SELECT creation_date FROM rentals.rentals WHERE rental_id = %s;'
            self.cursor.execute(SQL4, [x,])
            rental_creation_date = self.cursor.fetchone()[0]

            SQL5 = 'SELECT availability_window FROM rentals.calendar_availability WHERE rental_id = %s;'
            self.cursor.execute(SQL5, [x,])
            availability_window = self.cursor.fetchone()[0].days

            SQL6 = 'SELECT preparation_time FROM rentals.calendar_availability WHERE rental_id = %s;'
            self.cursor.execute(SQL6, [x,])
            preparation_time = self.cursor.fetchone()[0].days

            SQL7 = 'SELECT advance_notice FROM rentals.calendar_availability WHERE rental_id = %s;'
            self.cursor.execute(SQL7, [x,])
            advance_notice = self.cursor.fetchone()[0].days

            SQL8 = 'SELECT same_day_booking_max_time FROM rentals.calendar_availability WHERE rental_id = %s;'
            self.cursor.execute(SQL8, [x,])
            same_day_booking_max_time = self.cursor.fetchone()[0]

            SQL10 = 'SELECT * FROM rentals.custom_pricing WHERE rental_id = %s;'
            self.cursor.execute(SQL10, [x,])
            custom_pricing = self.cursor.fetchall()

            max_reservation_possible = datetime.today() + timedelta(days=availability_window)
            number_of_reservations = random.choice([*range(20,40)])
            final_data = []
            assembled_data = []


            #initiate data creation
            for num in ([*range(1, number_of_reservations)]):

                SQL9 = 'SELECT price FROM rentals.rentals WHERE rental_id = %s;'
                self.cursor.execute(SQL9, [x,])
                price = self.cursor.fetchone()[0]
                price = float(price.replace('$', ''))

                #choose traveler
                traveler = random.choice(user_ids)
                if traveler == rental_host_user_id:
                    while True:
                        traveler = random.choice(user_ids)
                        if traveler != rental_host_user_id:
                            break

                #choose reservation duration and temp_set_price
                possible_durations = [1,2,3,4,5,6,7,7,7,7,7,8,10,30,31,30,31,35,37,15,14,13]
                SQLF1 = 'SELECT min_stay, max_stay FROM rentals.rentals WHERE rental_id = %s;'
                self.cursor.execute(SQLF1, [x,])
                max_min_stay = self.cursor.fetchall()
                min_stay = (max_min_stay[0][0]).days
                max_stay = (max_min_stay[0][1]).days
                
                while True:
                    reservation_duration = random.choice(possible_durations)
                    if min_stay <= reservation_duration <= max_stay:
                        break
                        
                    
                weekly_discount_bool = False
                monthly_discount_bool = False

                if 7 <= reservation_duration < 30:
                    SQL11 = 'SELECT weekly_discount FROM rentals.rentals WHERE rental_id = %s;'
                    self.cursor.execute(SQL11, [x,])
                    weekly_discount = self.cursor.fetchone()[0]
                    if weekly_discount != None:
                        weekly_discount = float(weekly_discount)
                        price = round((price - (price*weekly_discount)),2)
                        weekly_discount_bool = True

                if reservation_duration >= 30:
                    SQL12 = 'SELECT monthly_discount FROM rentals.rentals WHERE rental_id = %s;'
                    self.cursor.execute(SQL12, [x,])
                    monthly_discount = self.cursor.fetchone()[0]
                    if monthly_discount != None:
                        monthly_discount = float(monthly_discount)
                        price = round((price - (price*monthly_discount)),2)
                        monthly_discount_bool = True

                last_minute_reservation = [True, False, False, False ]
                last_minute_reservation = random.choice(last_minute_reservation)
                last_minute_bool = False

                if last_minute_reservation == True and reservation_duration < 7:
                    SQL12 = 'SELECT last_minute_discount FROM rentals.rentals WHERE rental_id = %s;'
                    self.cursor.execute(SQL12, [x,])
                    last_minute_discount = self.cursor.fetchone()[0]
                    if last_minute_discount != None:
                        last_minute_discount = float(last_minute_discount)
                        price = round((price - (price*last_minute_discount)),2)
                        last_minute_bool = True

                #define_number of guests and charges
                guest_fee = False
                guests_max_exceeded = [True, False, False, False]
                guests_max_exceeded = random.choice(guests_max_exceeded)
                if guests_max_exceeded == True:
                    SQL13 = 'SELECT extra_guest_daily_fee FROM rentals.rentals WHERE rental_id = %s;'
                    self.cursor.execute(SQL13, [x,])
                    extra_guest_fee = self.cursor.fetchone()[0]
                    extra_guest_fee = float(extra_guest_fee.replace('$', ''))
                    num_extra_guests = random.choice([1,2,3,5,1,2,3])
                    extra_daily_guest_fee = extra_guest_fee * num_extra_guests
                    if extra_daily_guest_fee != 0.00:
                        price = price + extra_daily_guest_fee
                        guest_fee = True
                    

                #define pet charges
                pet_fee = False
                pet_fee_applied = [True, False, False, False]
                pet_fee_applied = random.choice(pet_fee_applied)
                if pet_fee_applied == True:
                    SQL14 = 'SELECT pet_fee FROM rentals.rentals WHERE rental_id = %s;'
                    self.cursor.execute(SQL14, [x,])
                    extra_pet_fee = self.cursor.fetchone()[0]
                    extra_pet_fee = float(extra_pet_fee.replace('$', ''))
                    if extra_pet_fee != 0.00:
                        pet_fee = True

                #prep final price with cleaning fee
                cleaning_fee_bool = False
                SQL15 = 'SELECT cleaning_fee FROM rentals.rentals WHERE rental_id = %s;'
                self.cursor.execute(SQL15, [x,])
                cleaning_fee = self.cursor.fetchone()[0]
                cleaning_fee = float(cleaning_fee.replace('$', ''))
                final_price = price * reservation_duration
                if cleaning_fee != 0.00:
                    cleaning_fee_bool = True
                if pet_fee == True:
                    final_price = final_price + pet_fee
                if cleaning_fee_bool == True:
                    final_price = final_price + cleaning_fee

                final_price = round(final_price, 2)
                price = round(price, 2)

                # Reservations dates
                
                while True:
                    reservation_starts = random_date_generator.date_between(start_date= rental_creation_date, end_date= max_reservation_possible)   
                    if last_minute_bool == True:
                        days_before_reservations = 1
                    else: 
                        days_before_reservations = random.choice([*range(1,60)])

                    if reservation_starts > datetime.today().date():
                        reservation_made_on = datetime.today().date() - timedelta(days=days_before_reservations)
                        break
                    else:
                        reservation_made_on = reservation_starts - timedelta(days=days_before_reservations)
                    if reservation_made_on > rental_creation_date:
                        break

                reservation_ends = reservation_starts + timedelta(days=reservation_duration)
         
                confirmed_by_host = False
                check_in = False
                check_out = False
                today = datetime.today().date()
                if reservation_ends < today:
                    confirmed_by_host = True
                    check_in = True
                    check_out = True

                if reservation_ends > today and reservation_starts < today:
                    confirmed_by_host = True
                    check_in = True
                    check_out = False

                if reservation_starts > today:
                    confirmed_host_options = [True, True, True, False]
                    confirmed_by_host = random.choice(confirmed_host_options)
                    check_in = False
                    check_out = False


                applied_fees_and_discounts = []
                number_of_extra_guests = 0
                if pet_fee == True:
                    applied_fees_and_discounts.append('pet_fee')
                if guest_fee == True:
                    applied_fees_and_discounts.append('guest_fee')
                    number_of_extra_guests = num_extra_guests
                if monthly_discount_bool == True:
                    applied_fees_and_discounts.append('monthly_discount')
                if weekly_discount_bool == True:
                    applied_fees_and_discounts.append('weekly_discount')
                if last_minute_bool == True:
                    applied_fees_and_discounts.append('last_minute_discount')
                if cleaning_fee_bool == True:
                    applied_fees_and_discounts.append('cleaning_fee')
                if not applied_fees_and_discounts:
                    applied_fees_and_discounts = None
            
                data_tuple = (x, traveler, price, (str(reservation_duration) + ' days'), reservation_made_on, reservation_starts, reservation_ends, confirmed_by_host, check_in, check_out, round(final_price,2), applied_fees_and_discounts, number_of_extra_guests)
                final_data.append(data_tuple)

            print(final_data)
            
            if custom_pricing != None:
                assembled_data0 = []
                for tpl in final_data:
                    for tpl2 in custom_pricing:
                        if tpl[5] <= tpl2[1] <= tpl[6]:
                            print('Custom Pricing')
                            price_new = float(tpl2[2].replace('$', ''))
                            reservation_duration_new = tpl2[3].days
                            reservation_duration = int(tpl[3].replace(' days', ''))

                            guest_fee_bool = False
                            if tpl[11] != None:
                                applied_fees_and_discounts = tpl[11]
                                if 'guest_fee' in applied_fees_and_discounts:
                                    SQLF2 = 'SELECT extra_guest_daily_fee FROM rentals.rentals WHERE rental_id = %s;'
                                    self.cursor.execute(SQLF2, [tpl[0],])
                                    extra_guest_fee = self.cursor.fetchone()[0]
                                    extra_guest_fee = float(extra_guest_fee.replace('$', ''))
                                    num_extra_guests = tpl[12]
                                    extra_daily_guest_fee = extra_guest_fee * num_extra_guests
                                    guest_fee_bool = True

                            if reservation_duration_new > reservation_duration:
                                reservation_duration = reservation_duration_new
                                if guest_fee_bool == True:
                                    price_new = price_new + extra_daily_guest_fee

                            else:
                                if guest_fee_bool == True:
                                    price_new = price_new + extra_daily_guest_fee

                            final_price = price_new * reservation_duration

                            reservation_starts = tpl2[1]
                            reservation_ends = reservation_starts + timedelta(days=reservation_duration)
                            if tpl[11] != None:
                                if guest_fee_bool:
                                    applied_fees_and_discounts = ['custom_pricing', 'guest_fee']
                                else:
                                    applied_fees_and_discounts = ['custom_pricing', ]
                            else:
                                if guest_fee_bool:
                                    applied_fees_and_discounts = ['custom_pricing', 'guest_fee']
                                else:
                                    applied_fees_and_discounts = ['custom_pricing', ]
                            while True:
                                if 'last_minute_discount' in applied_fees_and_discounts:     
                                    days_before_reservations = 1
                                else: 
                                    days_before_reservations = random.choice([*range(1,60)])

                                reservation_made_on = reservation_starts - timedelta(days=days_before_reservations)
                                if reservation_made_on > rental_creation_date:
                                    break

                            cleaning_fee_bool = False
                            SQLG1 = 'SELECT cleaning_fee FROM rentals.rentals WHERE rental_id = %s;'
                            self.cursor.execute(SQLG1, [tpl[0],])
                            cleaning_fee = self.cursor.fetchone()[0]
                            cleaning_fee = float(cleaning_fee.replace('$', ''))
                            if cleaning_fee != 0.00:
                                cleaning_fee_bool = True
                            if cleaning_fee_bool == True:
                                final_price = final_price + cleaning_fee
                                applied_fees_and_discounts.append('cleaning_fee')

                            confirmed_by_host = False
                            check_in = False
                            check_out = False
                            today = datetime.today().date()
                            if reservation_ends < today:
                                confirmed_by_host = True
                                check_in = True
                                check_out = True

                            if reservation_ends > today and reservation_starts < today:
                                confirmed_by_host = True
                                check_in = True
                                check_out = False

                            if reservation_starts > today:
                                confirmed_host_options = [True, True, True, False]
                                confirmed_by_host = random.choice(confirmed_host_options)
                                check_in = False
                                check_out = False

                            tpl = (tpl[0], tpl[1], price_new, (str(reservation_duration) + ' days'), reservation_made_on, reservation_starts, reservation_ends, confirmed_by_host, check_in, check_out, round(final_price, 2), applied_fees_and_discounts, tpl[12])
                            break
                    assembled_data0.append(tpl)
                final_data = assembled_data0

            assembled_data1 = []
            # reservation start tuple[5]
            # reservation ends tuple[6]
            for tpl in final_data:
                good_date = True
                for tpl2 in final_data:
                    if tpl == tpl2:
                        continue
                    else:
                        if tpl2[5] <= tpl[5] <= tpl2[6]:
                            print('Bad Date')
                            good_date = False
                        if tpl2[5] <= tpl[6] <= tpl2[6]:
                            print('Bad Date')
                            good_date = False
                    
                if good_date == True:
                    assembled_data1.append(tpl)
            final_data = assembled_data1

            assembled_data2 = []
            for tpl in final_data:
                good_date = True
                SQL16 = 'SELECT reservation_starts, reservation_ends FROM rentals.reservations WHERE guest_id= %s;'
                self.cursor.execute(SQL16, [tpl[1],])
                user_reservations = self.cursor.fetchall()
                if user_reservations != None:
                    for tpl2 in user_reservations:
                        if tpl2[0] <= tpl[5] <= tpl2[1]:
                            print('Bad Previous Date')
                            good_date = False
                        if tpl2[0] <= tpl[6] <= tpl2[1]:
                            print('Bad Previous Date')
                            good_date = False
            
                if good_date == True:
                    assembled_data2.append(tpl)
            final_data = assembled_data2

            for d in final_data:    
                SQL17 = 'INSERT INTO rentals.reservations( \
                        rental_id, guest_id, daily_price, \
                        reservation_duration, reservation_made_on, \
                        reservation_starts, reservation_ends, \
                        confirmed_by_host, check_in, check_out, total_price, \
                        applied_fees_and_discounts, number_of_extra_guests) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
                list_to_pass = list(d)
                print(list_to_pass)
                self.cursor.execute(SQL17, list_to_pass)
                self.conn.commit()
                
           
        print('done')

    def rental_reviews(self):    
        SQL1 = 'SELECT reservation_id, rental_id, guest_id FROM rentals.reservations;'
        self.cursor.execute(SQL1)
        reservations = self.cursor.fetchall()
        for x in reservations:
            words = [*range(10,30)]
            words = random.choice(words)
            message = (lorem.words(words))
            rating = [1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5 ,5, 3, 3.5, 4, 4.5]
            rating = random.choice(rating)
            SQL2 = 'INSERT INTO rentals.reviews( \
	        reservation_id, rental_id, guest_id, comment_text, rating) \
	        VALUES (%s, %s, %s, %s, %s);'
            list_to_pass = [x[0], x[1], x[2], message, rating]
            print(list_to_pass)
            self.cursor.execute(SQL2, list_to_pass)
            self.conn.commit()
            
    def erase_rating(self):
        SQL1 = 'SELECT rental_id FROM rentals.rentals;'
        self.cursor.execute(SQL1)
        rental_id = [x[0] for x in (self.cursor.fetchall())]   
        for x in rental_id:
            SQL2 = 'UPDATE rentals.rentals \
	                SET rating= %s WHERE rental_id = %s;'
            list_to_pass = [None, x]
            self.cursor.execute(SQL2, list_to_pass)
            self.conn.commit()

        
if __name__ == '__main__':
  
    database = DatabaseConn()

    # url = '/Users/ricardocolindres/Documents/IUBH/Project_Build_Data_Mart/Rentals_DB/DataSources/LoadRentals/rental_mockup_data.json'
    # data1 = json.load(open(url))
    database.rental_reviews()





