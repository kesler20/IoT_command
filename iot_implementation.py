from random import randint
import json
import random
import time
from config import *
import AWSIoTPythonSDK
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import pandas as pd

x_value = 0
total_1 = 1000
total_2 = 1080 
t1s = -6
t1e = 6
t2s = -7
t2e = 6.8
trend_1 = 1000
trend_2 = 1080


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

    def __init__(self, client_ID: str, data: pd.DataFrame):
        super().__init__(client_ID)
        self.data = data


def check(checker: Controller, t1a, y1a, t2a, y2a):  # input for function is (n)
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

    # leave to avoid any errors in line 112 where you convert it to a dataframe to use the iat syntax
    info = {
        "ctrl1": [x1],
        "ctrl2": [x2],
    }

    print('the current control command', info)
    checker.publish_data('pump/control', json.dumps(info))


def application_callback(client, user_data, message):
    global app
    try:
        data = str(message.payload)[
            str(message.payload).find('b')+1:].replace("'", '')
        info = json.loads(data)
        check(app, info['trend_1'], info['total_1'],
                         info['trend_2'], info['total_2'])
        # the check function publishes the data to the control topic
    except ValueError as err:
        print(err)
    except TypeError as err:
        print(err)


def controller_callback(client, user_data, message):
    global controller

    try:
        data = str(message.payload)[
            str(message.payload).find('b')+1:].replace("'", '')
        controller.data = pd.DataFrame(data=json.loads(data))
        print('the controller state', controller.data)
    except ValueError as err:
        print(err)


info = {
    "x_value": [x_value],
    "total_1": [total_1],
    "total_2": [total_2],
    "trend_1": [trend_1],
    "trend_2": [trend_2]
}

controller = Controller('pumpID', pd.DataFrame(data=info))
app = Device('appId')

controller.subscribe_to_topic('pump/control', controller_callback)  # subscribe
app.subscribe_to_topic('pump/pressure', application_callback)
while True:
    try:
        info = {
            "x_value": x_value,
            "total_1": total_1,
            "total_2": total_2,
            "trend_1": trend_1,
            "trend_2": trend_2
        }

        controller.publish_data('pump/pressure', json.dumps(info))
        print(x_value, total_1, trend_1, total_2, trend_2)

        # assuming that after the data is published the subscribe function of the app is triggered
        x_value += 1

        data = controller.data
        # manipulate the data from the callback
        print('Control 1: ' + str(data.iat[0, 0]) +
                ' Control 2: ' + str(data.iat[0, 1]))

        total_1 = total_1 + \
            random.randint(t1s, t1e) - (data.iat[0, 0] * (t1e-t1s)/16)
        total_2 = total_2 + \
            random.uniform(t2s, t2e) - (data.iat[0, 1] * (t2e-t2s)/2)

        trend_1 = trend_1 + (t1e-t1s)/2+t1s
        trend_2 = trend_2 + (t2e-t2s)/2+t2s

        time.sleep(2)  # in seconds
    except AWSIoTPythonSDK.exception.AWSIoTExceptions.subscribeTimeoutException:
        pass
