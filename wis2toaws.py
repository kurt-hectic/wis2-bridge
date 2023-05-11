import logging
import time
import sys
import threading
import time
import json
import traceback
import ssl
import os

import paho.mqtt.client as mqtt_paho

from uuid import uuid4
from awscrt import mqtt
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv

from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

from datetime import datetime

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=logging.INFO, 
    handlers=[ logging.FileHandler("debug.log"), logging.StreamHandler()] )

CERT = r"./certs/wis2_bridge_new.cert.pem"
KEY = r"./certs/wis2_bridge_new.private.key"
CA = r"./cas/AmazonRootCA1.pem"

#load_dotenv()


TOPICS = os.getenv("TOPICS").split(",")

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    logging.warning("Connection interrupted. error: {}".format(error))

# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    logging.info("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        logging.info("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)

def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    logging.info("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            logging.error("Server rejected resubscribe to topic: {}".format(topic))


def on_connect_wis(client, userdata, flags, rc):
    """
    set the bad connection flag for rc >0, Sets onnected_flag if connected ok
    also subscribes to topics
    """
    logging.info("Connected flags:"+str(flags)+" topics:"+str(TOPICS)+" result code: "+str(rc))    
    topics = [(topic,0) for topic in TOPICS]
    client_wis2.subscribe(topics)

def on_subscribe(client,userdata,mid,granted_qos):
    """removes mid values from subscribe list"""
    logging.info("in on subscribe callback result "+str(mid))
    client.subscribe_flag=True

def on_message(client, userdata, msg):
    topic=msg.topic
    logging.debug(f"message received with topic {topic}")
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    #logging.debug("message received")
    message_routing(client,topic,m_decode)

def on_disconnect(client, userdata, rc):
    logging.info("connection to broker "+client.broker," has been lost")
    client.connected_flag=False
    client.disconnect_flag=True

def message_routing(client,topic,msg):
    #logging.debug("message_routing")
    logging.debug("routing topic: "+topic)
    logging.debug("routing message: "+msg)
    
    #topic=topic.replace("sig/","")
    
    #topic="sdk/test/python"
    #topic="bfa/ouagadougou_met_centre/data/core/weather/surface-based-observations/synop"
    
    topic_levels = topic.split("/")

    # only take levels 4 to 12, if they are there. Levels 1-3 are fix for the moment
    topic_new = "/".join(topic_levels[3:9]) if len(topic_levels) > 10 else "/".join(topic_levels[3:])
    topic_new = topic_levels[0] + "/" + topic_new

    msg = json.loads(msg)
    msg["_meta"] = { "time_received" : datetime.now().isoformat() , "broker" : host , "topic" : topic }

    msg = json.dumps(msg)
      
    logging.debug(f"publishing topic {topic_new} with length {len(topic_levels)} and message length {len(msg)} and {msg}")
    logging.debug(f"publishing message {msg}")
    client_aws.publish(topic=topic_new,payload=msg,qos=mqtt.QoS.AT_LEAST_ONCE)
        
def create_wis2_connection(host,port):
    logging.info(f"creating wis2 connection to {host}:{port}")
    
    client = mqtt_paho.Client(client_id="wis2_bridge_new", transport="tcp",
         protocol=mqtt_paho.MQTTv311, clean_session=False)
                         
    client.username_pw_set(os.getenv("WIS_USERNAME"),os.getenv("WIS_PASSWORD"))
    if port == 8883:
        client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)
    
    client.on_message = on_message
    client.on_connect = on_connect_wis
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    
    #properties=Properties(PacketTypes.CONNECT)
    #properties.SessionExpiryInterval=30*60 # in seconds
    
    client.connect(host,
                   port=port,
                   #clean_start=mqtt_paho.MQTT_CLEAN_START_FIRST_ONLY,
                   #properties=properties,
                   keepalive=60)
    
    return client
    
def create_aws_connection():
    logging.info("creating AWS connection")
    return mqtt_connection_builder.mtls_from_path( 
        endpoint = "a3alzazats3klu-ats.iot.eu-central-1.amazonaws.com", 
        client_id = "wis2_bridge_new",
        on_connection_interrupted = on_connection_interrupted,
        on_connection_resumed  = on_connection_resumed,
        cert_filepath = CERT,
        pri_key_filepath = KEY,
        ca_filepath = CA
    )
                
# start
try:
    logging.info("creating connection to AWS")
    client_aws = create_aws_connection()
    connect = client_aws.connect()
    connect.result()
    logging.info("connected to AWS")    
    
    host = os.getenv("BROKER_HOST")
    port = int(os.getenv("BROKER_PORT"))
    logging.info("creating connection to WIS2")
    client_wis2 = create_wis2_connection(host,port)
    logging.info("connected to WIS2")

    client_wis2.loop_forever() #start loop
    logging.info("loop")

except Exception as e:
    logging.error("connection failed "+str(e))
    traceback.print_exc()
