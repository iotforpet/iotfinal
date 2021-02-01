import json
import time
from datetime import datetime, timedelta
import numpy as np
import paho.mqtt.client as PahoMQTT
import requests
import random
import thingspeak
class MyMQTT:
    def __init__(self, broker, port, notifier, petID):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.petID = petID
        self._paho_mqtt = PahoMQTT.Client("weight_Aggregator", False)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived

    def myOnConnect(self, paho_mqtt, userdata, flags, rc):
        pass
        # print ("Connected to message broker with result code: " + str(rc))

    def myOnMessageReceived(self, paho_mqtt, userdata, msg):
        self.notifier.notify(msg.topic, msg.payload)

    def myPublish(self, weig_id, name, time,data):
        pw_topic = 'pet/' + str(self.petID) + '/weight/' + weig_id + '/aggregated_weig'
        js = {"Sensor_Name": weig_id, "time": time, "value": data}
        print(js)
        self._paho_mqtt.publish(pw_topic, json.dumps(js), 2)

    def mySubscribe(self, topicRawWeig, qos=2):
        self._paho_mqtt.subscribe(topicRawWeig, qos)

    def start(self):
        print(self.port)
        self._paho_mqtt.connect(self.broker, int(self.port))
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.disconnect()
        self._paho_mqtt.loop_stop()


class weigDataAggregator:

    def __init__(self):
        self.systemFile = open("baseConfig.json")
        self.systemInfo = json.loads(self.systemFile.read())
        self.systemFile.close()
        self.dc_searchby = 'sensors'
        self.device_search = 'W-Sensors'
        self.mqttStatus = 0
        self.initSystem()

    def initSystem(self):
        self.deviceID = self.systemInfo["deviceID"]
        self.catalogURL = self.systemInfo["catalogURL"]
        # Resource details
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
        self.subscribeToTopics()
        self.aggregate_Data()

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
            "data": ["MQTT", "last_update", "DataCollection","ThingSpeak_weight",self.device_search]
        }
        serviceResp = json.loads(requests.post(self.scUrl, json.dumps(servReq)).text)

        if serviceResp["Result"] == "success":
            self.mqtt_broker = serviceResp["Output"]["MQTT"]["mqtt_broker"]
            self.mqtt_port = serviceResp["Output"]["MQTT"]["mqtt_port"]
            self.service_lastUpdate = serviceResp["Output"]["last_update"]
            self.frequency = serviceResp["Output"][self.device_search]["Frequency"]
            self.insertDataAPI = serviceResp["Output"]["DataCollection"]
            self.thinkAPI = serviceResp["Output"]["ThingSpeak_weight"]["API"]
            self.thinkAPIWriteKey = serviceResp["Output"]["ThingSpeak_weight"]["write_key"]
            self.thinkAPIReadKey = serviceResp["Output"]["ThingSpeak_weight"]["read_key"]
            self.thinkAPIChannelID = serviceResp["Output"]["ThingSpeak_weight"]["channel_ID"]
        else:
            print("couldnt recover service details. Trying again")
            time.sleep(60)
            self.serviceConfigurations()

    def startSystem(self):
        self.weig_readings = {i["ID"]: [] for i in self.weigSensors}
        self.agg_weig_readings = {i["ID"]: [] for i in self.weigSensors}

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

    def subscribeToTopics(self):
        for i in self.weigSensors:
            self.topicRawPwr = 'pet/' + str(self.petID) + '/weight/' + i['ID'] + '/petWet'
            self.myMqtt.mySubscribe(self.topicRawPwr)

    def notify(self, topic, msg):
        try:
            weigSensorID = topic.split("/")[3]
            self.weig_readings[weigSensorID].append(float((json.loads(msg)['value'])))
        except KeyError:
            print("Yet to add the sensor")

    # aggregate data got from the collector and publish to central controller, insert to database and thinkspeak
    def aggregate_Data(self):
        while True:
            for idx, weigSen in enumerate(self.weigSensors):
                if len(self.weig_readings[weigSen["ID"]]) >= self.frequency:
                    self.agg_weig_readings[weigSen["ID"]].append(
                        np.mean(self.weig_readings[weigSen["ID"]][:self.frequency])
                    )
                    del self.weig_readings[weigSen["ID"]][:self.frequency]
                    if len(self.agg_weig_readings[weigSen["ID"]]) == 1:
                        if "pet" in weigSen["Name"]:
                            oneHourAvg_w2 = round(np.mean(self.agg_weig_readings[weigSen["ID"]]),2)
                            dt = datetime.now() - timedelta(hours=1)
                            self.myMqtt.myPublish(weigSen["ID"], weigSen['Name'],str(dt.strftime('%Y-%m-%dT%H:%M:%S.%f')),
                            oneHourAvg_w2)
                            sensorData = json.dumps({
                                "call": "insert",
                                "data": {
                                    "date": dt.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                                    "sensorType": self.device_search,
                                    "sensorID": weigSen["ID"],
                                    "sensorName": weigSen["Name"],
                                    "sensorData": oneHourAvg_w2,
                                    "avgtime": 60,
                                    "unit": "Kg",
                                    "Status": None
                                }
                            })
                            print(weigSen["ID"])
                            print(sensorData)

                            try:
                                requests.post(self.insertDataAPI["DataInsertAPI"], sensorData)
                            except:
                                pass
                            del self.agg_weig_readings[weigSen["ID"]][:]
                        else:
                            oneHourAvg_w1 = round(np.mean(self.agg_weig_readings[weigSen["ID"]]), 2)
                            dt = datetime.now() - timedelta(hours=1)
                            self.myMqtt.myPublish(weigSen["ID"], weigSen['Name'],
                                                  str(dt.strftime('%Y-%m-%dT%H:%M:%S.%f')),
                                                  oneHourAvg_w1)
                            sensorData = json.dumps({
                                "call": "insert",
                                "data": {
                                    "date": dt.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                                    "sensorType": self.device_search,
                                    "sensorID": weigSen["ID"],
                                    "sensorName": weigSen["Name"],
                                    "sensorData": oneHourAvg_w1,
                                    "avgtime": 60,
                                    "unit": "grams",
                                    "Status": None
                                }
                            })
                            print(weigSen["ID"])
                            print(sensorData)

                            try:
                                requests.post(self.insertDataAPI["DataInsertAPI"], sensorData)
                            except:
                                pass
                            del self.agg_weig_readings[weigSen["ID"]][:]
                try:
                    if(oneHourAvg_w1 is not None and oneHourAvg_w2 is not None):
                    # channel = thingspeak.Channel(id=self.thinkAPIChannelID, write_key=self.thinkAPIWriteKey,
                    #                              api_key=self.thinkAPIReadKey)
                    # response = channel.update({'field1 ': str(oneHourAvg)})
                        tPayload = "field1=" + str(oneHourAvg_w2)+"&field2=" + str(oneHourAvg_w1)
                        print(self.thinkAPI + "&" + tPayload)
                        print(requests.get(self.thinkAPI + "&" + tPayload))
                        oneHourAvg_w2 = None
                        oneHourAvg_w1 = None
                except:
                    pass
            time.sleep(2)


if __name__ == "__main__":
    aggregate = weigDataAggregator()
    aggregate.aggregate_Data()