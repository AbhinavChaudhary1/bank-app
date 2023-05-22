import sqlite3
import csv
import database_management as dm
# Object of this class is used to write the email.
from email.message import EmailMessage
import ssl  # For security of email and information inside it.
import smtplib  # Used to send email
import datetime
import logging 


class DatabaseAcess:
    def __init__(self, dbname):
        self.dbname = dbname
        self.conn = sqlite3.connect(f"SOURCE/RESOURCE/{self.dbname}") #setting a connection to connect to the databse  
        self.cursor = self.conn.cursor() #creating a cursor to execute the commands in the databse

    def create_table(self, table_name): #creation of table inside the database
        self.cursor.execute(dm.table_creation(table_name))
        self.conn.commit()

    def insert_data(self, csvfile, table_name): #insertion of data into the table

        with open(f"SOURCE/RESOURCE/{csvfile}", 'r') as file:
            csv_reader = csv.reader(file)
            # moving the csv_reader to next line as first line contains field names
            columns_heading = next(csv_reader)
            self.cursor.executemany(dm.data_insert(table_name), csv_reader)
            self.conn.commit()

    def show(self, tablename):
        self.cursor.execute(dm.showall(tablename))
        items = self.cursor.fetchall()[0]
        print(items)
        self.conn.commit()

    def showcolumn(self, tablename):
        print(f'\nColumns in {tablename} table:')
        datas = self.cursor.execute(dm.showall(tablename))
        for column in datas.description:
            print(column[0])
        self.conn.commit()

    def add_column(self, tablename): # adding new columns to table
        alter_table = dm.table_alter(tablename)
        for query in alter_table:
            self.cursor.execute(query)
        self.conn.commit()

    def mark_dulicate_data(self, tablename): #marking duplicate data
        duplicates = self.cursor.execute(
            dm.get_duplicate(tablename)).fetchall()
        if len(duplicates) > 0:
            tran_id_list = []
            for i in range(len(duplicates)):
                tran_id_list.append(duplicates[i][0])
            self.cursor.execute(dm.update_duplicate(tablename, tran_id_list))

        else:
            print('NO Duplicate Data')
        self.conn.commit()

    def mark_suspicious_data(self, tablename): #marking suspicious data
        suspicious = self.cursor.execute(
            dm.get_suspicious(tablename)).fetchall()
        if len(suspicious) > 0:
            sus_tran_id_list = []
            for i in range(len(suspicious)):
                sus_tran_id_list.append(suspicious[i][0])
            self.cursor.execute(dm.update_suspicious(
                tablename, sus_tran_id_list))
        else:
            print('NO Suspicious data')
        self.conn.commit()

    def yearwise_transaction(self, tablename): #yearwise total transaction
        per_year_tran = self.cursor.execute(
            dm.no_of_transaction(tablename)).fetchall()
        header = ['Year', 'Total Transactions',
                  'Most occurred transaction type']
        with open('SOURCE/RESOURCE/yearwise_transaction.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(per_year_tran)

        self.conn.commit()

    def credit_interest(self, tablename): #crediting interest to eligible person
        max_trans_id_data = self.cursor.execute(
            dm.max_trans_id(tablename)).fetchall()
        if len(max_trans_id_data) > 0:
            tran_id_list = []
            for i in range(len(max_trans_id_data)):
                tran_id_list.append(max_trans_id_data[i][0])

            self.cursor.execute(dm.interest_credit(tablename, tran_id_list))
        else:
            print('No such data')
        self.conn.commit()

    def pensioner_interest_credit_data(self, tablename):
        after_credit_data = self.cursor.execute(
            dm.pensioner_interest_addition_data(tablename)).fetchall()
        header = ['id', 'trans_id', 'account_id', 'type', 'operation', 'amount', 'balance', 'k_symbol', 'bank',
                  'account', 'year', 'month', 'day', 'fulldate', 'fulltime', 'fulldatewithtime', 'flag', 'email']
        with open('SOURCE/RESOURCE/bonus_to_pensioners.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(after_credit_data)

        self.conn.commit()

    def auto_email(self, tablename): # auto email feature  
        email_sender = 'sender email id'  # Write email id of sender here
        email_password = 'password'  # To be set in email id account
        

        Final_day = datetime.datetime.now() + datetime.timedelta(days=10)
        last_date = (Final_day.strftime("%x"))
        

        user_data = self.cursor.execute(dm.auto_mail(tablename)).fetchall()
        print(user_data)
        if len(user_data) > 0:
            for i in range(len(user_data)):
                user_acc_id = user_data[i][0]
                user_balance = user_data[i][1]
                user_email = user_data[i][2]

                sub = 'BANK ALERTS: DO NOT REPLY'
                body = """Dear {}
                Your current balance is {}. As per the bank policy, customers need to maintain an average minimum monthly balance of Rs. 800 to keep their savings bank account active. Please make sure you maintain the minimum balance in your account by {} or you face a penalty.
                Thanks and Regards,
                National Bank
                'Note: This is a system generated mail, please don't reply'.""".format(user_acc_id, user_balance, last_date)

                mail = EmailMessage()
                mail['From'] = email_sender
                mail['To'] = "{}".format(user_email)
                mail['Subject'] = sub
                mail.set_content(body)

                context = ssl.create_default_context()
            
            # setting connection to server
                with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                    smtp.login(email_sender, email_password)
                    smtp.sendmail(email_sender, user_email, mail.as_string())

                self.conn.commit()
        else:
            print('No such Data')
            self.conn.commit()