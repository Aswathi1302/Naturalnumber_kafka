from kafka import KafkaConsumer
import mysql.connector
import sys
try:
    mydb=mysql.connector.connect(host='localhost',user='root',password='',database='evenkafka')
except mysql.connector.Error as e:
   sys.exit("mysql connection error")   
mycursor=mydb.cursor()


bootstrap_server=["localhost:9092"]

topic="naturalnumber"

consumer=KafkaConsumer(topic,bootstrap_servers = bootstrap_server)

for i in consumer:
    print(str(i.value.decode()))
    random_even = int(i.value.decode())
    if(random_even % 2 ) == 0:
       sql="INSERT INTO `even`(`evennmbr`) VALUES (%s)"
       data=(random_even,)
       mycursor.execute(sql,data)
       mydb.commit()
       print("even number add to db" ,random_even)
    

