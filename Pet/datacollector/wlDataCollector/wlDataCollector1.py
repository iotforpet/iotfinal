import json
import random as rd
import time
import Adafruit_DHT as dht
import paho.mqtt.client as MQTT
import requests
import RPi.GPIO as GPIO
import random
from datetime import datetime
class MyMQTT:
    def __init__(self, broker, port, notifier, petID):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.petID = petID
        self._paho_mqtt = MQTT.Client("Readwlht", False)
        self._paho_mqtt.on_connect = self.myOnConnect


    def myOnConnect(self, paho_mqtt, userdata, flags, rc):
        pass
        # print ("Connected to message broker with result code: " + str(rc))

    def myPublish(self, wl_id, name, data):
        pw_topic = 'pet/' + str(self.petID) + '/wlht/'+wl_id+'/petWl'
        # print(pw_topic)
        js = {"Wlht": name, "value": data,"dt":str(datetime.now())}
        print(js)
        self._paho_mqtt.publish(pw_topic, json.dumps(js), 2)

    def start(self):
        print(self.port)
        self._paho_mqtt.connect(self.broker, int(self.port))
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.disconnect()
        self._paho_mqtt.loop_stop()


class wlDataCollector:
    def __init__(self):
        self.systemFile = open("baseConfig.json")
        self.systemInfo = json.loads(self.systemFile.read())
        self.systemFile.close()
        self.dc_searchby = 'sensors'
        self.device_search = 'WL-Sensors'
        self.mqttStatus = 0
        self.startTime = int(time.time())
        self.initSystem()

    def initSystem(self):
        self.deviceID = self.systemInfo["deviceID"]
        self.catalogURL = self.systemInfo["catalogURL"]
        #Resource details
        self.getInfo = json.loads(requests.get(self.catalogURL + "/getinfo/" + self.deviceID).text)
        if self.getInfo["Result"] == "success":
            self.info = self.getInfo["Output"]
            self.petID = self.info["PetID"]
            self.dcUrl = self.info["Devices"][self.deviceID]["DC"]
            self.scUrl = self.info["Devices"][self.deviceID]["SC"]
            self.catalog_lastUpdate = self.getInfo["Output"]["lastUpdate"]
        else:
            print("System Initialisation Failed due to Resource Catalog Issues")
            time.sleep(60)
            self.initSystem()
        self.deviceConfigurations()
        self.serviceConfigurations()
        self.startSystem()
        self.checkCatalogRegister()
        self.collect_wl_data()


    def deviceConfigurations(self):
        # Device Details
        self.device_details = json.loads(requests.get(self.dcUrl
                                                      + self.petID + "/"
                                                      + self.deviceID + "/"
                                                      + self.dc_searchby + "/"
                                                      + self.device_search).text)
        if self.device_details["Result"] == "success":
            self.wlSensors = self.device_details["Output"]["installed{}".format(self.device_search)]
            self.device_lastUpdate = self.device_details["Output"]["last_update"]

        else:
            print("Couldnt recover device details")
            time.sleep(60)
            self.deviceConfigurations()

    def serviceConfigurations(self):
        # Service Details
        servReq = {
            "call": "getService",
            "petID": self.petID,
            "deviceID": self.deviceID,
            "data": ["MQTT", "last_update"]
        }
        serviceResp = json.loads(requests.post(self.scUrl, json.dumps(servReq)).text)

        if serviceResp["Result"] == "success":
            self.mqtt_broker = serviceResp["Output"]["MQTT"]["mqtt_broker"]
            self.mqtt_port = serviceResp["Output"]["MQTT"]["mqtt_port"]
            self.service_lastUpdate = serviceResp["Output"]["last_update"]
        else:
            print("couldnt recover service details. Trying again")
            time.sleep(60)
            self.serviceConfigurations()

    def startSystem(self):

        self.myMqtt = MyMQTT(self.mqtt_broker, self.mqtt_port, self, self.petID)
        self.myMqtt.start()

    def checkCatalogRegister(self):
        # checking device registration in catalog
        for i in self.wlSensors:
            if i['ID'] not in self.info["Devices"][self.deviceID]["Sensors"]:
                print("device not registered in Resource Catalog. Registering now..")
                reg_device = {
                    "call": "adddevices",
                    "PetID": self.petID,
                    "data": {"type": "Sensors", "deviceID": self.deviceID, "values": [i["ID"]]}
                }
                requests.post(self.catalogURL, reg_device)

    def setup(self,GPIO_TRIGGER,GPIO_ECHO):
        GPIO.setmode(GPIO.BCM)
        # Set pins as output and input
        GPIO.setup(GPIO_TRIGGER, GPIO.OUT)  # Trigger
        GPIO.setup(GPIO_ECHO, GPIO.IN)  # Echo
        # Set trigger to False (Low)
        GPIO.output(GPIO_TRIGGER, False)
        # Allow module to settle
        time.sleep(0.5)

    def measure_average(self,speedsound,GPIO_TRIGGER,GPIO_ECHO):
        # This function takes 3 measurements and
        # returns the average.

        distance1 = self.measure(speedsound,GPIO_TRIGGER,GPIO_ECHO)
        time.sleep(0.1)
        distance2 = self.measure(speedsound,GPIO_TRIGGER,GPIO_ECHO)
        time.sleep(0.1)
        distance3 = self.measure(speedsound,GPIO_TRIGGER,GPIO_ECHO)
        distance = distance1 + distance2 + distance3
        distance = distance / 3
        return distance

    def measure(self,speedsound,GPIO_TRIGGER,GPIO_ECHO):
        # This function measures a distance
        GPIO.output(GPIO_TRIGGER, True)
        # Wait 10us
        time.sleep(0.00001)
        GPIO.output(GPIO_TRIGGER, False)
        start = time.time()

        while GPIO.input(GPIO_ECHO) == 0:
            start = time.time()

        while GPIO.input(GPIO_ECHO) == 1:
            stop = time.time()

        elapsed = stop - start
        distance = (elapsed * 34300) / 2

        return distance

    def active_inactive(self,active,i):
        # update in device catalog
        if active == 1:
            activeData = {
                "call": "updateDevices",
                "petID": self.petID,
                "deviceID": self.deviceID,
                "catalogURL": self.catalogURL,
                "data": {"sensor": self.device_search,
                         "sensorID": i['ID'],
                         "properties": {"active": 1,"Name": i['Name']}}
            }
            requests.post(self.dcUrl, json.dumps(activeData))

        else:
            i["active"] = 0
            # update in device catalog
            inactiveData = {
                "call": "updateDevices",
                "petID": self.petID,
                "deviceID": self.deviceID,
                "catalogURL": self.catalogURL,
                "data": {"sensor": self.device_search,
                         "sensorID": i['ID'],
                         "properties": {"active": 0,"Name": i['Name']}}
            }
            requests.post(self.dcUrl, json.dumps(inactiveData))

    # collect data got from the sensor and publish to aggregator
    def collect_wl_data(self):
        wlInactivity = [0 for i in range(len(self.wlSensors))]
        inActivityCheckCounter = 0
        while True:
            self.deviceConfigurations()
            inActivityCheckCounter += 1
            for idx, i in enumerate(self.wlSensors):
                try:
                    if i['web_active'] == 1:
                        self.setup(i['TRIGGER'], i['ECHO'])
                        wlht = self.measure(0,i['TRIGGER'], i['ECHO'])
                        # wlht  = random.randrange(2,10)
                        print("Distance : {0:5.1f}".format(wlht))
                        time.sleep(1)
                        self.myMqtt.myPublish(
                            i['ID'],
                            i['Name'],
                             round(wlht,2))
                    else:
                        print("Water Sensor:" + i['ID'] + " not Actived by user")
                        time.sleep(5)
                        self.deviceConfigurations()
                except:
                    wlInactivity[idx] = wlInactivity[idx] + 1
                    if wlInactivity[idx] > 3:
                        wlInactivity[idx] =0
                        # GPIO.cleanup()
                        self.active_inactive(0,i)
                    print ("wlht Sensor:" + i['ID'] + " not Active")

            if inActivityCheckCounter >= 5:
                print("checking Inactive")
                self.deviceConfigurations()
                inActivityCheckCounter = 0
                inActiveWl = filter(lambda curwl: curwl['active'] == 0, self.wlSensors)
                for wl in inActiveWl:
                    try:
                        if wl['web_active'] == 1:
                            self.setup(wl['TRIGGER'], wl['ECHO'])
                            wlht = self.measure(0, wl['TRIGGER'], wl['ECHO'])
                            # wlht  = random.randrange(2,15)
                            print("Distance : {0:5.1f}".format(wlht))
                            if wlht is not None:
                                self.active_inactive(1, wlht)
                                self.wlSensors[wl['ID']]["active"] = 1
                        else:
                            print("Water Sensor:" + wl['ID'] + " not Actived by user")
                            time.sleep(5)
                            self.deviceConfigurations()
                    except:
                        pass
            time.sleep(3)
if __name__ == '__main__':
    collect = wlDataCollector()
    collect.collect_wl_data()