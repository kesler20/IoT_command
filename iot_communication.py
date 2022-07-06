from random import randint
import time
from config import *
import AWSIoTPythonSDK
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import pandas as pd

upper_threshold = 6
lower_threshold = 4

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
        self.client.connect(keepAliveIntervalSecond=900)

    def publish_data(self, topic: str, payload):
        '''topic format -> thing/measurement/property
        i.e. topic : sensor/temperature/high'''
        self.client.publish(topic, payload, 0)

    def subscribe_to_topic(self, topic: str, custom_callback):
        '''Callback functions should be of the following form
        def callback(client,used_data,message):
            function(message)
        where message has properties message.payload and message.topic'''
        self.client.subscribe(topic, 1, custom_callback)

    def tear_down(self, topic):
        self.client.disconnect()
        self.client.unsubscribe(topic)

class Controller(Device):
    '''A controller is a device with an internal state for storing and updating

    control command'''

    def __init__(self, client_ID: str, data: int):
        super().__init__(client_ID)
        self.data = data

def application_callback(client, user_data, message):
    global app
    try:
        data = str(message.payload)[
            str(message.payload).find('b')+1:].replace("'", '')
        data = int(data)
        control_command = 0
        if data > upper_threshold:
            control_command = -1
        elif data < lower_threshold:
            control_command = 1
        else:
            pass
        app.data = control_command
        print('the current control command', app.data)
        app.publish_data('pump/control', app.data)
    except ValueError as err:
        print(err)

def controller_callback(client,user_data,message):
    global controller

    try:
        data = str(message.payload)[
            str(message.payload).find('b')+1:].replace("'", '')
        controller.data += int(data)
        print('the controller state', controller.data)
    except ValueError as err:
        print(err)


controller = Controller('deviceId', randint(0, 10))
app = Controller('appId', 0)

controller.subscribe_to_topic('pump/control', controller_callback)  # subscribe
app.subscribe_to_topic('pump/pressure', application_callback)
while True:
    try:
        controller.publish_data('pump/pressure', controller.data)
        time.sleep(2)
    except AWSIoTPythonSDK.exception.AWSIoTExceptions.subscribeTimeoutException:
        pass

