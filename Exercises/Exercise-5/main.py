from numpy import insert
import psycopg2
import pandas as pd
import os
from csv import reader
def main():
    host = 'localhost'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    query_accounts = """Create  table IF NOT EXISTS accounts(customer_id INTEGER PRIMARY KEY, 
                                                        first_name VARCHAR(256), 
                                                        last_name VARCHAR(256), 
                                                        address_1 VARCHAR(256), 
                                                        address_2 VARCHAR(256), 
                                                        city VARCHAR(256), 
                                                        state VARCHAR(256), 
                                                        zip_code INTEGER, 
                                                        join_date DATE)"""
    query_products = """Create  table IF NOT EXISTS products(product_id INTEGER PRIMARY KEY, 
                                        product_code INTEGER, 
                                        product_description VARCHAR(256))"""                                                     
    query_transaction = """Create  table  IF NOT EXISTS transactions(transaction_id VARCHAR(256) PRIMARY KEY, 
                                                                transaction_date DATE, 
                                                                product_id INTEGER,
                                                                product_code VARCHAR(256) ,
                                                                product_description VARCHAR(256),
                                                                quantity INTEGER, 
                                                                account_id INTEGER,
                                                                FOREIGN KEY (product_id) REFERENCES products (product_id),
                                                                FOREIGN KEY (account_id) REFERENCES accounts (customer_id))
                                                                """
    queries = [query_accounts,query_products,query_transaction]
    for query in queries:                                                           
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
    
    for root,dirs,files in os.walk('data',topdown=True):
        for file in files:
            table = file.split('.')[0]
            with open(root+'/'+file,'r') as file:
                    x = reader(file)
                    header = next(x)
                    for i in x:
                        i = tuple(i)    # i will contain the values to insert into the database
                        query = "insert into {} values{}".format(table,i)
                        cur.execute(query)
                        conn.commit()
    
    
                                                   
if __name__ == '__main__':
    main()
