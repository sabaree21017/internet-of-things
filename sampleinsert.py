import sqlite3 # for sqlite 3
import datetime # we need this library to accomplish date and time functions
import paho.mqtt.client as mqtt  #for fetching published data in mqtt

# MQTT connections & feeds
mqtt_address='192.168.43.218'    #for mqtt server address                   
mqtt_user=''                  #mqtt user id
mqtt_password=''              #mqtt password
mqtt_topic='/feeds/ppm'      #mqtt topic for pollution sensor

#function for inserting pollution data in sqlite
def insertPollutionDataSQLite(node_id,ppm):   
    try:
        conn=sqlite3.connect('streetlight.db')  #connect with streetlight database
        cursor=conn.cursor()                    #open cursor for database connection
        print("sucessfully connected to DB")   
        
        times=datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S:%f") # form current time stamp 
        
        #sql_query string formation the string will be passed as parameter to excute sqlscript
        
        sql_query= "delete from pollutionsensor; INSERT into pollutionsensor(node_id,ppm,timestamp) values("+str(node_id)+","+ppm+",'"+times+"');"
        
        count = cursor.executescript(sql_query) # excute the script
        
        conn.commit()  # commit the sql connection
        cursor.close()  #close the cursor
        print("pollution sensor record(s) inserted successfully")
        
    # expection block for database and sql query

    except sqlite3.Error as error:
        print("fail to insert record",error)
    #close all the connections
        
    finally:
        if conn:
            conn.close()# close the database connection
            print("database closed")

# connect mqtt server for ppm data feed 
def on_connect(client,userdata,flags,rc):
    #print('mqtt connected with result code'+stc(rc))
    client.subscribe(mqtt_topic)

# ON sucessfull mqtt connection fetch ppm value and insert in sqlite database table
def on_message(client,userdata,msg):
    insertPollutionDataSQLite(1,msg.payload.decode())
    print(msg.topic+''+str(msg.payload))

def main():
    mqtt_client=mqtt.Client()
    mqtt_client.username_pw_set(mqtt_user,mqtt_password)
    mqtt_client.on_connect=on_connect
    mqtt_client.on_message=on_message
    
    #connect to mqtt server
    mqtt_client.connect(mqtt_address,1883)
    mqtt_client.loop_forever()  #listen for published data forever

if __name__=='__main__':
    print('MQTT to InfluxDB bridge')
    main()