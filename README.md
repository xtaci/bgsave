# redis persistence service
periodically dump records from redis.      
the format of the record is defined as :         

key(tablname:record_id) -> value(msgpack)

# environment variables:
* REDIS_HOST : eg: 127.0.0.1:6379    
* MONGODB_URL : eg: mongodb://172.17.42.1/mydb
* NSQD_HOST :  eg: http://172.17.42.1:4151
