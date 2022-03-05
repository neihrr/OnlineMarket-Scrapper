import redis

CarrefourData = redis.Redis()

def isDuplicate(prod_attribute):   
    data = CarrefourData.get(prod_attribute)
   
    if(data != None):
        return data
    else:
        return False  



def insertRedis(prod_sku, price_date):
   CarrefourData.set(prod_sku, price_date)
    

