from decode_transaction_data import DecodeTransactionData 
import logging
import logging_data
import constant as c 
from database_access import DatabaseAcess

logging_data.logging.info('Decoding transaction data file')
decoding_data = DecodeTransactionData(c.tran_data_encoded,c.tran_data_decoded,c.cln_data)
decoding_data.decode_data()
logging_data.logging.info('File is decoded successfully')

data_acc = DatabaseAcess(c.dbname)
data_acc.create_table(c.tran_tab)
logging_data.logging.info('Table sucessfully created in the database')


data_acc.insert_data(c.cln_data,c.tran_tab)
data_acc.add_column(c.tran_tab)
data_acc.show(c.tran_tab)
data_acc.showcolumn(c.tran_tab)
data_acc.mark_dulicate_data(c.tran_tab)
data_acc.mark_suspicious_data(c.tran_tab)

data_acc.yearwise_transaction(c.tran_tab)
data_acc.credit_interest(c.tran_tab)
data_acc.pensioner_interest_credit_data(c.tran_tab)

data_acc.auto_email(c.tran_tab)