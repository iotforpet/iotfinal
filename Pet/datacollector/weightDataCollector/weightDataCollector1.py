import json
import random as rd
import time
import Adafruit_DHT as dht
import paho.mqtt.client as MQTT
import requests
import RPi.GPIO as GPIO
from hx711 import HX711
from datetime import datetime
# from datacollector.weightDataCollector.hx711 import HX711
import random
class MyMQTT:
    def __init__(self, broker, port, notifier, petID):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.petID = petID
        self._paho_mqtt = MQTT.Client("Readweight", False)
        self._paho_mqtt.on_connect = self.myOnConnect


    def myOnConnect(self, paho_mqtt, userdata, flags, rc):
        pass
        # print ("Connected to message broker with result code: " + str(rc))

    def myPublish(self, weig_id, name, data):
        pw_topic = 'pet/' + str(self.petID) + '/weight/'+weig_id+'/petWet'
        # print(pw_topic)
        js = {"Weight": name, "value": data,"dt":str(datetime.now())}
        print(js)
        self._paho_mqtt.publish(pw_topic, json.dumps(js), 2)

    def start(self):
        print(self.port)
        self._paho_mqtt.connect(self.broker, int(self.port))
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.disconnect()
        self._paho_mqtt.loop_stop()


class weigDataCollector:
    def __init__(self):
        self.systemFile = open("baseConfig.json")
        self.systemInfo = json.loads(self.systemFile.read())
        self.systemFile.close()
        self.dc_searchby = 'sensors'
        self.device_search = 'W-Sensors'
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
        self.collect_weig_data()


    def deviceConfigurations(self):
        # Device Details
        self.device_details = json.loads(requests.get(self.dcUrl
                                                      + self.petID + "/"
                                                      + self.deviceID + "/"
                                                      + self.dc_searchby + "/"
                                                      + self.device_search).text)
        if self.device_details["Result"] == "success":
            self.weigSensors = self.device_details["Output"]["installed{}".format(self.device_search)]
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
        for i in self.weigSensors:
            if i['ID'] not in self.info["Devices"][self.deviceID]["Sensors"]:
                print("device not registered in Resource Catalog. Registering now..")
                reg_device = {
                    "call": "adddevices",
                    "PetID": self.petID,
                    "data": {"type": "Sensors", "deviceID": self.deviceID, "values": [i["ID"]]}
                }
                requests.post(self.catalogURL, reg_device)

    def setup(self,hx,offset,scale):
        """
        code run once
        """
        hx.set_offset(offset)
        hx.set_scale(scale)

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
    def collect_weig_data(self):
        weigInactivity = [0 for i in range(len(self.weigSensors))]
        inActivityCheckCounter = 0
        while True:
            self.deviceConfigurations()
            inActivityCheckCounter += 1
            for idx, i in enumerate(self.weigSensors):
                try:
                    if i['web_active'] == 1:
                        hx = HX711(i['GPIO'], i['SCK'])
                        self.setup(hx,i['offset'],i['scale'])
                        weight = hx.get_grams()
                        hx.power_down()
                        time.sleep(.001)
                        hx.power_up()
                        if(weight>0):
                            if("pet" in i['Name']):
                                self.myMqtt.myPublish(
                                    i['ID'],
                                    i['Name'],
                                     round(weight/1000,2))
                            else:
                                self.myMqtt.myPublish(
                                    i['ID'],
                                    i['Name'],
                                    round(weight, 2))
                    else:
                        print("Weight Sensor:" + i['ID'] + " not Actived by user")
                        time.sleep(5)
                        self.deviceConfigurations()
                except:
                    weigInactivity[idx] = weigInactivity[idx] + 1
                    if weigInactivity[idx] > 3:
                        weigInactivity[idx] =0
                        # GPIO.cleanup()
                        self.active_inactive(0,i)
                    print ("Weight Sensor:" + i['ID'] + " not Active")

            if inActivityCheckCounter >= 5:
                print("checking Inactive")
                self.deviceConfigurations()
                inActivityCheckCounter = 0
                inActiveWeig = filter(lambda curweig: curweig['active'] == 0, self.weigSensors)
                for weig in inActiveWeig:
                    try:
                        if weig['web_active'] == 1:
                            hx = HX711(i['GPIO'], i['SCK'])
                            self.setup(hx, i['offset'], i['scale'])
                            weight = hx.get_grams()
                            hx.power_down()
                            time.sleep(.001)
                            hx.power_up()
                            if weight is not None:
                                self.active_inactive(1, weig)
                                self.weigSensors[weig['ID']]["active"] = 1
                        else:
                            print("Weight Sensor:" + weig['ID'] + " not Actived by user")
                            time.sleep(5)
                            self.deviceConfigurations()
                    except:
                        pass
            time.sleep(2)
if __name__ == '__main__':
    collect = weigDataCollector()
    collect.collect_weig_data()