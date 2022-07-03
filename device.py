import json
from random import randint
import time
from config import *
import AWSIoTPythonSDK
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import random 
import pandas as pd

TOPIC_CONTROL = 'control'
TOPIC_DATA = 'data'
data = pd.DataFrame({'ctrl1': [0], 'ctrl2': [0]})


class Device(object):
    '''A class as an interface for any IoT thing which can be registered

    such as sensors actuators etc..   
    these are considered AWS IoT MQTT Clients using TLSv1.2 Mutual Authentication'''

    def __init__(self, client_ID: str):

        self.client: AWSIoTMQTTClient = AWSIoTPyMQTT.AWSIoTMQTTClient(
            client_ID)
        self.configure_client()

    def configure_client(self):
        self.client.configureEndpoint(END_POINT, 8883)
        self.client.configureCredentials(
            PATH_TO_ROOT_CA, PATH_TO_PRIVATE_KEY, PATH_TO_CERTIFICATE)
        self.client.connect()

    def publish_data(self, topic: str, payload):
        '''topic format -> thing/measurement/property

        i.e. topic : sensor/temperature/high'''
        self.client.publish(topic, payload, 1)

    def subscribe_to_topic(self, topic: str, custom_callback):
        '''Callback functions should be of the following form

        def callback(client,used_data,message):
            function(message)

        where message has properties message.payload and message.topic'''
        self.client.subscribe(topic, 1, custom_callback)

    def tear_down(self, topic):
        self.client.disconnect()
        self.client.unsubscribe(topic)

def check(checker: Device, t1a, y1a, t2a, y2a): # input for function is (n)
    if t1a > y1a+20:
        x1 = -1
    elif t1a < y1a-20:
        x1 = 1
    else:
        x1 = 0

    if t2a > y2a+5:
        x2 = -1
    elif t2a < y2a-5:
        x2 = 1
    else:
        x2 = 0

    info = {
        "ctrl1": x1,
        "ctrl2": x2,
    }

    checker.publish_data(TOPIC_CONTROL, str(info))

    return x1, x2

def call_back(client, user_data, message):
    global data 

    msg = str(message.payload)[
        str(message.payload).find('b')+1:].replace("'", '')
    
    print(type(data))
    data['ctrl1'] = [data['ctrl1']]
    data['ctrl2'] = [data['ctrl2']]
    print(data)

checker = Device('checkerID')
number_generator = Device('numberID')
data_reader = Device('dataID')

x_value = 0
total_1 = 1000
total_2 = 1080
t1s = -6
t1e = 6
t2s = -7
t2e = 6.8
trend_1 = 1000
trend_2 = 1080

while True:
    try:
        info = {
            "x_value": x_value,
            "total_1": total_1,
            "total_2": total_2,
            "trend_1": trend_1,
            "trend_2": trend_2
        }

        number_generator.publish_data(TOPIC_DATA,str(info))
        check(checker, trend_1, total_1, trend_2, total_2)

        #print(x_value, total_1, trend_1, total_2, trend_2,)

        x_value += 1

        data_reader.subscribe_to_topic(TOPIC_CONTROL, call_back)

        # print('Control 1: ' + str(data.iat[0,0]) + ' Control 2: ' + str(data.iat[0,1]))

        # total_1 = total_1 + random.randint(t1s, t1e) - (data.iat[0,0] * (t1e-t1s)/16)
        # total_2 = total_2 + random.uniform(t2s, t2e) - (data.iat[0,1] * (t2e-t2s)/2)

        # trend_1 = trend_1 + (t1e-t1s)/2+t1s
        # trend_2 = trend_2 + (t2e-t2s)/2+t2s
    except AWSIoTPythonSDK.exception.AWSIoTExceptions.subscribeTimeoutException:
        pass

# pump.tear_down('pump/pressure')
