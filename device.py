import time
from config import *

import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

#set device up a a conterxt manager
# class File(object):
    	
#     def __init__(self, filename, mode):
#         self.filename = filename 
#         self.mode = mode 
#     def __enter__(self):
#         self.file = open(self.filename, self.mode)
#         return self.file
#     def __exit__(self, exec_type, exec_val, traceback):
#         self.file.close()

# with File('code_snippets.txt', 'w') as f:
# 	f.write('this is the file we are using)

# print(f.close)


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


def call_back(client, user_data, message):
    print(str(message.payload)[str(message.payload).find('b')+1:].replace("'",''))


pump = Device('pumpID')
compressor = Device('compressorID')

for i in range(10):
    pump.publish_data('pump/pressure', 'pressure form the pump')
    compressor.subscribe_to_topic('pump/pressure', call_back)

pump.tear_down('pump/pressure')
compressor.tear_down('pump/pressure')
