from dotenv  import load_dotenv
from pprint  import pprint as pp
from paho    import mqtt
from sys     import path
from os.path import abspath, dirname

load_dotenv()
import paho.mqtt.client as paho
import datetime
import time 
import csv
import os


#------------------------------------#

# the directory where the database is defined to the path
root_dir = dirname(dirname(dirname(abspath(__file__))))
print(root_dir + "/maticas-database")
path.append(root_dir + "/maticas-database")

from db.db_handlers.connections_handler import *

class MqttClient():

    """
        This class gets messages from the MQTT broker 
        and retrieves them.
    """

    def __init__(self,
                 mqtt_broker: str,
                 mqtt_port: int, 
                 mqtt_username: str,
                 mqtt_password: str,
                 mqtt_client_id: str, 
                 database_conn_handler: ConnectionsHandler):

        print("creating client ...")
        self.mqttBroker = mqtt_broker
        self.database_conn_handler = database_conn_handler

        # instanciates the client
        self.client = paho.Client( client_id = mqtt_client_id,
                                   userdata  = None,
                                   protocol  = paho.MQTTv5 )

        self.client.on_message = self._on_message
        self.client.on_connect = self.on_connect
        print("\t on_connect set")
        print("\t on_message set")

        # enables secure connection
        #self.client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)

        print("loging into broker ...")
        self.client.username_pw_set(username = mqtt_username,
                                    password = mqtt_password)

        #connects asynchronously to the broker
        self.client.connect_async(self.mqttBroker, mqtt_port, keepalive=60)

        # by then calling client.loop_start() the client will run a loop 
        # in a background thread. This loop will take care of calling
        # client.on_connect() and client.on_message()
        self.client.loop_start()

        print("connected to broker.")
        print("-"*60)

    #-------------------------------------------------------------------------#
    #                                                                         #
    #-------------------------------------------------------------------------#

    def get_subscribe_topics(self) -> None:

        """
            This method gets and adds the subscribe topics 
            from the .env file.
        """

        # separates the topics and puts them in a list
        self.subscribe_topics = self.database_conn_handler.get_all_topics()

        if len(self.subscribe_topics["topic"]) == 0:
            self.subscribe_topics = []

        self.subscribe_topic = self.subscribe_topics["topic"]

        print("Found subscribe topics:")
        print(self.subscribe_topic)
        

    def set_on_message(self, func):
        self.client.on_message = func


    def set_send_message(self, func):
        self.client.on_message = func


    #-------------------------------------------------------------------------#
    #                                                                         #
    #-------------------------------------------------------------------------#

    def on_connect(self, client, userdata, flags, rc, properties=None):

        print("Connected with result code "+str(rc))

        # subscribes to the topics
        self.get_subscribe_topics()

        for topic in self.subscribe_topics:
            self.client.subscribe(topic, qos = 2)
            print(f"Subscribed to topic: {topic}.\n")
        
        print("-"*60)


    def send_message(self, topic: str, message: str, qos = 1) -> None:

        self.client.publish(topic, message, qos)
        time.sleep(0.1) 


    def _on_message(self, client, userdata, message) -> None:

        # decodes the message
        msg = str(message.payload.decode("utf-8"))       
        msg = msg.strip(' ')

        # gets the topic
        topic = message.topic                           

        #recieved message, its topic and its qos 
        print(f"Message received: {msg} from topic: {topic} with QoS: {message.qos}")
        
