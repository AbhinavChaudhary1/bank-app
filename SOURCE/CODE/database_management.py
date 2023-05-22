
import constant as c


def table_creation(tablename): #table creation query
    table_creation=(f'''CREATE TABLE {tablename}
                  (id INTEGER,
                   trans_id TEXT,
                   account_id TEXT,
                   type TEXT,
                   operation TEXT,
                   amount REAL,
                   balance REAL,
                   k_symbol TEXT,
                   bank TEXT,
                   account TEXT,
                   year INTEGER,
                   month INTEGER,
                   day INTEGER,
                   fulldate TEXT,
                   fulltime TEXT,
                   fulldatewithtime TEXT)
                  ''')
    return table_creation

def data_insert(tablename): #data insertion query
    data_insert=(f"INSERT INTO {tablename} VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    return data_insert

def showall(tablename):
    sall = (f"SELECT * FROM {tablename}")
    return sall

def table_alter(tablename): #column addition query
    add_email=(f"ALTER TABLE {tablename} ADD COLUMN email TEXT")
    add_flag=(f"ALTER TABLE {tablename} ADD COLUMN flag TEXT")
    alter_list=[]
    alter_list.append(add_email)
    alter_list.append(add_flag)
    return alter_list

def get_duplicate(tablename): #query to get duplicate data
    get_duplicate=(f"""SELECT trans_id, account_id FROM {tablename} 
                   GROUP BY account_id HAVING COUNT(trans_id)>1""")
    return get_duplicate

def update_duplicate(tablename,t_id): #query to mark duplicate data  
    update_duplicate="""UPDATE {0} SET flag = 'Duplicate Transaction id' 
                     WHERE trans_id IN ({1})""".format(tablename,', '.join('\''+ item +'\'' for item in t_id))
    return update_duplicate

def get_suspicious(tablename): #query to mark suspicious transaction
    get_suspicious_tran=(f"""SELECT trans_id FROM {tablename} WHERE operation = ''""")
    return get_suspicious_tran

def update_suspicious(tablename,t_id): #query to mark suspicious transaction
    update_suspicious_tran="""UPDATE {0} SET flag = flag || ',Supicious Transaction'
                           WHERE trans_id IN ({1})""".format(tablename,', '.join('\''+ item +'\'' for item in t_id))
    return update_suspicious_tran

def no_of_transaction(tablename): #query to get no. of transaction per year
    no_of_transaction=(f"""SELECT year, COUNT(trans_id) as Total_Transactions, (SELECT operation FROM
                      (SELECT COUNT(trans_id) as COUNT, operation FROM {tablename} as a 
                      where a.year = b.year GROUP BY operation ORDER BY 1 DESC) LIMIT 1) as Most_Occurred_Transaction_Type 
                      FROM {tablename} as b GROUP BY year""")
    return no_of_transaction

def max_trans_id(tablename):
    max_trans_id = (f"""SELECT MAX(trans_id) FROM {tablename} WHERE year = 2018 and k_symbol = 'Old Age Pension' 
                    GROUP BY account_id""")
    return max_trans_id

def interest_credit(tablename,t_lst): #query to credit interest for pensioners
    credit_interest="""UPDATE {0} SET balance = balance*1.05,
                       flag = flag || ',Pensioner Bonus Credited'
                       WHERE trans_id IN ({1})""".format(tablename,', '.join('\''+ item +'\'' for item in t_lst))
    return credit_interest 
    
def pensioner_interest_addition_data(tablename): 
    final_balance = (f"""SELECT * FROM {tablename} WHERE flag LIKE'%Pensioner Bonus Credited%'""")
    return final_balance
 

def auto_mail(tablename): #query to select users having balance less than 800
    send_mail= (f"SELECT account_id, balance, email FROM {tablename} WHERE balance < 800 and year = 2018")
    return send_mail