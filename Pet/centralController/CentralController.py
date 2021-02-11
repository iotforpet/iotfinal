import json
import time
from datetime import datetime
import math
import numpy as np
import paho.mqtt.client as PahoMQTT
import requests
import urllib3
import ssl


# Mqtt Methods
class MyMQTT:
    def __init__(self,name, broker, port, notifier, petID):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.petID = petID
        self._paho_mqtt = PahoMQTT.Client(name, False)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived

    def myOnConnect(self, paho_mqtt, userdata, flags, rc):
        print ("Connected to message broker with result code: " + str(rc))

    def myOnMessageReceived(self, paho_mqtt, userdata, msg):
        self.notifier.notify(msg.topic, msg.payload)

    def myPublish(self, plug_id, pt_name, data, topic=""):
        pt_topic = topic
        js = {"petID": pt_name,"value": data,"dt":datetime.now()}
        print(js)
        self._paho_mqtt.publish(pt_topic, json.dumps(js).encode('utf8'), 2)

    def mySubscribe(self, topic, qos=2):
        self._paho_mqtt.subscribe(topic, qos)

    def start(self):
        print(self.port)
        self._paho_mqtt.connect(self.broker, int(self.port))
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.disconnect()
        self._paho_mqtt.loop_stop()



class CentralController:

    def __init__(self):
        self.systemFile = open("baseConfig.json")
        self.systemInfo = json.loads(self.systemFile.read())
        self.systemFile.close()
        self.device_search = 'all'
        self.curDevices = []
        self.initSystem()

    # init system
    def initSystem(self):
        self.deviceID = self.systemInfo["deviceID"]
        self.catalogURL = self.systemInfo["catalogURL"]
        getInfo = json.loads(requests.get(self.catalogURL + "/getinfo/" + self.deviceID).text)
        if getInfo["Result"] == "success":
            info = getInfo["Output"]
            self.catalog_Sensors = info["Devices"][self.deviceID]["Sensors"]
            self.petID = info["PetID"]
            self.dcUrl = info["Devices"][self.deviceID]["DC"]
            self.scUrl = info["Devices"][self.deviceID]["SC"]
            self.serverURL = info["serverURL"]
            self.catalog_lastUpdate = info["lastUpdate"]
        else:
            print("System Initialisation Failed due to Resource Catalog Issues")
            time.sleep(60)
            self.initSystem()

        self.deviceConfigurations()
        self.serviceConfigurations()
        self.petDetails()
        self.startSystem()
        self.checkCatalogRegister()
        self.subscribeToTopics()
        self.decisionMaking()

    def deviceConfigurations(self):
        # Device Details
        device_details = json.loads(requests.get(self.dcUrl
                                                 + self.petID + "/"
                                                 + self.deviceID + "/"
                                                 + self.device_search).text)
        if device_details["Result"] == "success":
            self.wLSensors= device_details["Output"]["WL-Sensors"]["installedWL-Sensors"]
            self.wLLastUpdate = device_details["Output"]["WL-Sensors"]["last_update"]
            self.tempSensors = device_details["Output"]["T-Sensors"]["installedT-Sensors"]
            self.tempLastUpdate = device_details["Output"]["T-Sensors"]["last_update"]
            self.wSensors = device_details["Output"]["W-Sensors"]["installedW-Sensors"]
            self.wLastUpdate = device_details["Output"]["W-Sensors"]["last_update"]
            self.curDevices.extend([i["ID"] for i in self.wSensors])
            self.curDevices.extend([i["ID"] for i in self.tempSensors])
            self.curDevices.extend([i["ID"] for i in self.wLSensors])

        else:
            print("Couldnt recover device details")
            time.sleep(60)
            self.deviceConfigurations()

        getDevUpdate = json.loads(requests.get(self.dcUrl
                                               + self.petID + "/"
                                               + self.deviceID + "/getlastupdate").text)
        self.device_lastUpdate = getDevUpdate["Output"]

    def serviceConfigurations(self):
        # Service Details
        try:
            serviceDetails = json.loads(requests.get(self.scUrl
                                                     + self.petID + "/"
                                                     + self.deviceID + "/all").text)
            if serviceDetails["Result"] == "success":
                self.mqtt_broker = serviceDetails["Output"]["MQTT"]["mqtt_broker"]
                self.mqtt_port = serviceDetails["Output"]["MQTT"]["mqtt_port"]
                self.telmqtt_broker = serviceDetails["Output"]["TelegramMQTT"]["mqtt_broker"]
                self.telmqtt_port = serviceDetails["Output"]["TelegramMQTT"]["mqtt_port"]
                self.sensorDataAPI = serviceDetails["Output"]["DataCollection"]["DataInsertAPI"]
                self.tempFrequency = serviceDetails["Output"]["T-Sensors"]["Frequency"]
                self.weightFrequency = serviceDetails["Output"]["W-Sensors"]["Frequency"]
                self.waterFrequency = serviceDetails["Output"]["WL-Sensors"]["Frequency"]
                self.service_lastUpdate = serviceDetails["Output"]["last_update"]
            else:
                print("Service details request failed")
                time.sleep(60)
                self.serviceConfigurations()

        except requests.exceptions.ConnectionError as e:
            print("Connection Failed with {}".format(e))
            time.sleep(120)
            self.serviceConfigurations()

    # pet Details
    def petDetails(self):
        self.pet_owner_details = json.loads(requests.get(self.serverURL
                                                 +"getPetUserDetails" + "/"
                                                 + self.petID).text)
        self.pet_data = json.loads(requests.get(self.serverURL
                                                         + "getPetDetails" + "/"
                                                         + self.pet_owner_details['Output'][0]["IDS"][0]['breed']).text)

    def startSystem(self):
        self.agg_sensor_readings = {i: [] for i in self.curDevices}
        self.proj_temp_readings = {i["ID"]: [] for i in self.tempSensors}
        self.proj_temp_date = {i["ID"]: [] for i in self.tempSensors}
        self.proj_w_readings = {i["ID"]: [] for i in self.wSensors}
        self.proj_w_date = {i["ID"]: [] for i in self.wSensors}
        self.proj_w_sensor_name = {i["ID"]: [] for i in self.wSensors}
        self.proj_wL_readings = {i["ID"]: [] for i in self.wLSensors}
        self.proj_wL_date = {i["ID"]: [] for i in self.wLSensors}
        print(self.agg_sensor_readings)
        self.myMqtt = MyMQTT("centralController",self.mqtt_broker, self.mqtt_port, self, self.petID)
        self.myMqtt.start()
        self.telMqtt = MyMQTT("TelegramMQTT",self.telmqtt_broker, self.telmqtt_port, self, self.petID)
        self.telMqtt.start()

    def checkCatalogRegister(self):

        # checking device registration in catalog
        for i in self.curDevices:
            if i not in self.catalog_Sensors:
                print("device not registered in Resource Catalog. Registering now..")
                reg_device = {
                    "call": "adddevices",
                    "HouseID": self.petID,
                    "data": {"type": "Sensors", "deviceID": self.deviceID, "values": [i["ID"]]}
                }
                json.loads(requests.post(self.catalogURL, reg_device).text)

    def identifySensorType(self, name, data=''):
        if 'WL_1' in name:
            sen = "wlht"
            top = "aggregated_wl"
        elif 'temp' in name:
            sen = "temperature"
            top = "aggregated_temp"
        else:
            sen = "weight"
            top = "aggregated_weig"
        return sen, top

    def subscribeToTopics(self):
        for i in self.curDevices:
            sen, top = self.identifySensorType(i)
            topic = 'pet/{}/{}/{}/{}'.format(self.petID, sen, i, top)
            self.myMqtt.mySubscribe(topic)

    def notify(self, topic, msg):
        try:
            dev = topic.split("/")[3]
            print(topic, msg)
            if 'wlht' in topic and len(self.agg_sensor_readings[dev]) == (60 / self.waterFrequency):
                self.proj_wL_readings[dev].append(float((json.loads(msg)['value'])))
                self.proj_wL_date[dev].append(str((json.loads(msg)['time'])))
                self.agg_sensor_readings[dev].pop(0)
            elif 'temp_1' in topic and len(self.agg_sensor_readings[dev]) == (60 / self.tempFrequency):
                self.proj_temp_readings[dev].append(float((json.loads(msg)['value'])))
                self.proj_temp_date[dev].append(str((json.loads(msg)['time'])))
                self.agg_sensor_readings[dev].pop(0)
            elif 'weight' in topic and len(self.agg_sensor_readings[dev])  == (60 / self.weightFrequency):
                self.proj_w_readings[dev].append(float((json.loads(msg)['value'])))
                self.proj_w_date[dev].append(str((json.loads(msg)['time'])))
                self.agg_sensor_readings[dev].pop(0)
            self.agg_sensor_readings[dev].append(float((json.loads(msg)['value'])))
            print(self.agg_sensor_readings)
        except KeyError:
            print("Yet to add the sensor")

    #  decision making publishing to telegram bot alert messages
    def decisionMaking(self):
        while True:
            self.pet_data_out = self.pet_data['Output'][0]
            for idx, temp in enumerate(self.proj_temp_readings):
                self.petDetails()
                if self.proj_temp_readings[temp] and len(self.agg_sensor_readings[temp]):
                    self.agg_sensor_readings[temp].pop(0)
                    high = self.pet_data_out['Temperature']['high']
                    low = self.pet_data_out['Temperature']['low']
                    das = str(datetime.strptime(self.proj_temp_date[temp][0],
                                                '%Y-%m-%dT%H:%M:%S.%f'))
                    if(np.mean(self.proj_temp_readings[temp])>=high):
                        self.telMqtt.myPublish("", self.petID, "Room Temperature is High",
                                               topic='petService/centralController/telegram/messages')
                        status = "HIGH"
                    elif (np.mean(self.proj_temp_readings[temp])<=low):
                        self.telMqtt.myPublish("", self.petID, "Room Temperature is Low",
                                               topic='petService/centralController/telegram/messages')
                        status = "LOW"
                    else:
                        status = "NORMAL"
                    try:
                        json.loads(requests.get(self.sensorDataAPI+ "/"
                                                + "updateStatus" + "/"
                                                + das+ "/"+status))
                    except:
                        pass
                    self.proj_temp_readings[temp].clear()
                    self.proj_temp_date[temp].clear()
                    self.agg_sensor_readings[temp].clear()
            for idx, wl in enumerate(self.proj_wL_readings):
                self.petDetails()
                if self.proj_wL_readings[wl] and len(self.agg_sensor_readings[wl]):
                    self.agg_sensor_readings[wl].pop(0)
                    high = self.pet_data_out['water_day']['high']
                    low = self.pet_data_out['water_day']['low']
                    das = str(datetime.strptime(self.proj_wL_date[wl][0],
                                                '%Y-%m-%dT%H:%M:%S.%f'))
                    if (np.mean(self.proj_wL_readings[wl]) >= high):
                        self.telMqtt.myPublish("", self.petID, "Water feed to pet is high",
                                               topic='petService/centralController/telegram/messages')
                        status = "HIGH"
                    elif (np.mean(self.proj_wL_readings[wl]) <= low):
                        self.telMqtt.myPublish("", self.petID,"Water feed to pet is low",
                                               topic='petService/centralController/telegram/messages')
                        status = "LOW"
                    else:
                        status = "NORMAL"
                    try:
                        json.loads(requests.get(self.sensorDataAPI + "/"
                                                + "updateStatus" + "/"
                                                + das + "/" + status))
                    except:
                        pass
                    self.proj_wL_readings[wl].clear()
                    self.proj_wL_date[wl].clear()
                    self.agg_sensor_readings[wl].clear()
            for idx, w in enumerate(self.proj_w_readings):
                self.petDetails()
                self.pet_weight = np.mean(self.proj_w_readings["W_2"])
                if w=="W_2":
                    if self.proj_w_readings[w] and len(self.agg_sensor_readings[w]):
                        self.agg_sensor_readings[w].pop(0)
                        if("Male" in self.pet_owner_details['Output'][0]["IDS"][0]['sex_of_animal']):
                            high = self.pet_data_out['Weight']['Male']['high']
                            low = self.pet_data_out['Weight']['Male']['low']
                        else:
                            high = self.pet_data_out['Weight']['Female']['high']
                            low = self.pet_data_out['Weight']['Female']['low']
                        das = str(datetime.strptime(self.proj_w_date[w][0],
                                                    '%Y-%m-%dT%H:%M:%S.%f'))
                        if (np.mean(self.proj_w_readings[w]) >= high):
                            self.telMqtt.myPublish("", self.petID, "Pet is over weight",
                                                   topic='petService/centralController/telegram/messages')
                            status = "HIGH"
                        elif (np.mean(self.proj_w_readings[w]) <= low):
                            self.telMqtt.myPublish("", self.petID, "Pet is under weight",
                                                   topic='petService/centralController/telegram/messages')
                            status = "LOW"
                        else:
                            status = "NORMAL"
                        try:
                            json.loads(requests.get(self.sensorDataAPI + "/"
                                                    + "updateStatus" + "/"
                                                    + das + "/" + status))
                        except:
                            pass
                        self.proj_w_readings[w].clear()
                        self.proj_w_date[w].clear()
                        self.agg_sensor_readings[w].clear()
                else:
                    if self.proj_w_readings[w] and len(self.agg_sensor_readings[w]):
                        self.agg_sensor_readings[w].pop(0)
                        weight4_5 = self.pet_data_out['Food_day']['4.5']
                        weight14 = self.pet_data_out['Food_day']['14']
                        weight27 = self.pet_data_out['Food_day']['27']
                        weight35 = self.pet_data_out['Food_day']['35']
                        weight40 = self.pet_data_out['Food_day']['40']
                        das = str(datetime.strptime(self.proj_w_date[w][0],
                                                    '%Y-%m-%dT%H:%M:%S.%f'))
                        if (self.pet_weight >= weight4_5 and  self.pet_weight<=weight14):
                            # self.telMqtt.myPublish("", self.petID, "Food per day is"+ str(weight4_5),
                            #                        topic='petService/centralController/telegram/messages')
                            status = str(weight4_5)
                        elif (self.pet_weight >= weight14 and  self.pet_weight<=weight27):
                            # self.telMqtt.myPublish("", self.petID, "Food per day is"+ str(weight14),
                            #                        topic='petService/centralController/telegram/messages')
                            status = str(weight14)
                        elif (self.pet_weight >= weight27 and self.pet_weight <= weight35):
                            # self.telMqtt.myPublish("", self.petID, "Food per day is" + str(weight27),
                            #                        topic='petService/centralController/telegram/messages')
                            status = str(weight27)
                        elif (self.pet_weight >= weight35 and self.pet_weight <= weight40):
                            # self.telMqtt.myPublish("", self.petID, "Food per day is" + str(weight35),
                            #                        topic='petService/centralController/telegram/messages')
                            status = str(weight35)
                        else:
                            # self.telMqtt.myPublish("", self.petID, "Food per day is" + str(weight40),
                            #                        topic='petService/centralController/telegram/messages')
                            status = str(weight40)
                        try:
                            json.loads(requests.get(self.sensorDataAPI + "/"
                                                    + "updateStatus" + "/"
                                                    + das + "/" + status))
                        except:
                            pass
                        self.proj_w_readings[w].clear()
                        self.proj_w_date[w].clear()
                        self.agg_sensor_readings[w].clear()
            time.sleep(5)

if __name__ == "__main__":
    controller = CentralController()
    controller.decisionMaking()
