from dotenv         import load_dotenv
from time           import sleep
from mqtt_client    import MqttClient
from os.path        import abspath, dirname
from sys            import path
import os
load_dotenv()

# the directory where the database is defined to the path
root_dir = dirname(dirname(dirname(abspath(__file__))))
print(root_dir + "/maticas-database")
path.append(root_dir + "/maticas-database")

from  db.db_handlers.users_db_connection   import *
from  db.db_handlers.connections_handler   import *

usr_conn = UsrDbConnection( db_host =  os.getenv("USR_DB_HOST"),
                            db_name =  os.getenv("USR_DB_NAME"),
                            db_user =  os.getenv("USR_DB_USER"),
                            db_password = os.getenv("USR_DB_PASSWORD"),
                            db_sslmode  = os.getenv("USR_DB_SSLMODE") )
conn_handler = ConnectionsHandler(usr_conn)


mqtt_conn = MqttClient( mqtt_broker     = os.environ['MQTT_IP'],
                        mqtt_port       = int(os.environ['MQTT_PORT']),
                        mqtt_username   = os.environ['MQTT_USERNAME'],
                        mqtt_password   = os.environ['MQTT_PASSWORD'],
                        mqtt_client_id  = os.environ['MQTT_CLIENT_ID'], 
                        database_conn_handler = conn_handler)

while True:

    sleep(0.1)

