import psycopg2
import myConfig
from datetime import datetime
import redisTransfer

cur = None

def create_tables():
    
    toCreate = (

        """
        CREATE TABLE price_history(
            
           
            profuct_sku NUMERIC,
            FOREIGN KEY (product_sku) REFERENCES data_carrefour(product_sku),
            product_price VARCHAR(20),
            price_date VARCHAR(50)
        )
       
        """,


        """
        CREATE TABLE data_carrefour(
             
            product_name VARCHAR(300),
            product_sku NUMERIC,
            product_price NUMERIC


        )
        """
        
    )

    conn=None

    try:
        #reading database configuration
        params = myConfig.config("creds")
        #connecting to postgresql
        conn = psycopg2.connect(**params)
        #creating a new cursor
        cur = conn.cursor()

        for c in toCreate:
            cur.execute(c)
        conn.commit() #saving the changes


    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()




def transfer():
    params = myConfig.config("creds")
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    for product in cur.fetchall():
        redisTransfer.insertRedis(product['prod_sku'],product['prod_name'], product['prod_price'],)
        

def insert_to_postgre_price_data(product_sku, product_price, price_date):
   

    sql = """ INSERT INTO price_history(product_sku, product_price,price_date)
    VALUES(%s, %s, %s)
    """

    conn = None
    
    
    try:
        params = myConfig.config("creds")
        conn = psycopg2.connect(**params)
        cur=conn.cursor()
        cur.execute(sql,(product_sku, product_price, price_date))
        conn.commit()
        cur.close()
    
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()    


def insert_to_postgre_all_data(product_name, product_sku , product_price, product_category):
    sql = """ INSERT INTO products_data_carrefour(product_name, product_sku, product_price, product_category) 
    VALUES(%s, %s, %s, %s) """

    conn = None
    
    
    try:
        params = myConfig.config("creds")
        conn = psycopg2.connect(**params)
        cur=conn.cursor()
        cur.execute(sql,(product_name,product_sku,product_price,product_category))       
        conn.commit()
        cur.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    



   



if __name__ == '__main__':
    create_tables()

