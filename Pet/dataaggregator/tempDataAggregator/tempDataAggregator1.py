import json
import time
from datetime import datetime, timedelta
import numpy as np
import paho.mqtt.client as PahoMQTT
import requests
import thingspeak

class MyMQTT:
    def __init__(self, broker, port, notifier, petID):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.petID = petID
        self._paho_mqtt = PahoMQTT.Client("temperature_Aggregator", False)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived

    def myOnConnect(self, paho_mqtt, userdata, flags, rc):
        pass
        # print ("Connected to message broker with result code: " + str(rc))

    def myOnMessageReceived(self, paho_mqtt, userdata, msg):
        self.notifier.notify(msg.topic, msg.payload)

    def myPublish(self, temp_id, name,time, data):
        pw_topic = 'pet/' + str(self.petID) + '/temperature/' + temp_id + '/aggregated_temp'
        js = {"Sensor_Name": temp_id,"time":time, "value": data}
        print(js)
        self._paho_mqtt.publish(pw_topic, json.dumps(js), 2)

    def mySubscribe(self, topicRawTemp, qos=2):
        self._paho_mqtt.subscribe(topicRawTemp, qos)

    def start(self):
        print(self.port)
        self._paho_mqtt.connect(self.broker, int(self.port))
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.disconnect()
        self._paho_mqtt.loop_stop()


class tempDataAggregator:

    def __init__(self):
        self.systemFile = open("baseConfig.json")
        self.systemInfo = json.loads(self.systemFile.read())
        self.systemFile.close()
        self.dc_searchby = 'sensors'
        self.device_search = 'T-Sensors'
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
            self.tempSensors = self.device_details["Output"]["installed{}".format(self.device_search)]
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
            "data": ["MQTT", "last_update", "DataCollection","ThingSpeak_temp", self.device_search]
        }
        serviceResp = json.loads(requests.post(self.scUrl, json.dumps(servReq)).text)

        if serviceResp["Result"] == "success":
            self.mqtt_broker = serviceResp["Output"]["MQTT"]["mqtt_broker"]
            self.mqtt_port = serviceResp["Output"]["MQTT"]["mqtt_port"]
            self.service_lastUpdate = serviceResp["Output"]["last_update"]
            self.frequency = serviceResp["Output"][self.device_search]["Frequency"]
            self.insertDataAPI = serviceResp["Output"]["DataCollection"]
            self.thinkAPI = serviceResp["Output"]["ThingSpeak_temp"]["API"]
            self.thinkAPIWriteKey = serviceResp["Output"]["ThingSpeak_temp"]["write_key"]
            self.thinkAPIReadKey = serviceResp["Output"]["ThingSpeak_temp"]["read_key"]
            self.thinkAPIChannelID = serviceResp["Output"]["ThingSpeak_temp"]["channel_ID"]
        else:
            print("couldnt recover service details. Trying again")
            time.sleep(60)
            self.serviceConfigurations()

    def startSystem(self):
        self.temp_readings = {i["ID"]: [] for i in self.tempSensors}
        self.agg_temp_readings = {i["ID"]: [] for i in self.tempSensors}

        self.myMqtt = MyMQTT(self.mqtt_broker, self.mqtt_port, self, self.petID)
        self.myMqtt.start()

    def checkCatalogRegister(self):
        # checking device registration in catalog
        for i in self.tempSensors:
            if i['ID'] not in self.info["Devices"][self.deviceID]["Sensors"]:
                print("device not registered in Resource Catalog. Registering now..")
                reg_device = {
                    "call": "adddevices",
                    "PetID": self.petID,
                    "data": {"type": "Sensors", "deviceID": self.deviceID, "values": [i["ID"]]}
                }
                requests.post(self.catalogURL, reg_device)

    def subscribeToTopics(self):
        for i in self.tempSensors:
            self.topicRawPwr = 'pet/' + str(self.petID) + '/temperature/' + i['ID'] + '/petTmp'
            self.myMqtt.mySubscribe(self.topicRawPwr)

    def notify(self, topic, msg):
        try:
            tempSensorID = topic.split("/")[3]
            self.temp_readings[tempSensorID].append(float((json.loads(msg)['value'])))
        except KeyError:
            print("Yet to add the sensor")
# aggregate data got from the collector and publish to central controller, insert to database and thinkspeak
    def aggregate_Data(self):
        while True:
            for idx, tempSen in enumerate(self.tempSensors):
                if len(self.temp_readings[tempSen["ID"]]) >= self.frequency:
                    self.agg_temp_readings[tempSen["ID"]].append(
                        np.mean(self.temp_readings[tempSen["ID"]][:self.frequency])
                    )
                    del self.temp_readings[tempSen["ID"]][:self.frequency]
                    if len(self.agg_temp_readings[tempSen["ID"]]) == 1:
                        oneHourAvg = round(np.mean(self.agg_temp_readings[tempSen["ID"]]),2)
                        dt = datetime.now() - timedelta(hours=1)
                        self.myMqtt.myPublish(tempSen["ID"], tempSen['Name'], str(dt.strftime('%Y-%m-%dT%H:%M:%S.%f')),
                        oneHourAvg)
                        sensorData = json.dumps({
                            "call": "insert",
                            "data": {
                                "date": dt.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                                "sensorType": self.device_search,
                                "sensorID": tempSen["ID"],
                                "sensorName": tempSen["Name"],
                                "sensorData": oneHourAvg,
                                "avgtime": 60,
                                "unit": "C",
                                "Status":None
                            }
                        })
                        print(sensorData)
                        try:
                        #     # channel = thingspeak.Channel(id=self.thinkAPIChannelID, write_key=self.thinkAPIWriteKey,
                        #     #                              api_key=self.thinkAPIReadKey)
                        #     # response = channel.update({'field1': str(oneHourAvg)})
                        #     # print(response)
                            tPayload = "field1=" + str(oneHourAvg)
                            print(self.thinkAPI + "&" + tPayload)
                            print(requests.get(self.thinkAPI + "&" + tPayload))
                        except:
                                pass
                        try:
                            requests.post(self.insertDataAPI["DataInsertAPI"], sensorData)
                        except:
                            pass
                        del self.agg_temp_readings[tempSen["ID"]][:]
            time.sleep(1)


if __name__ == "__main__":
    aggregate = tempDataAggregator()
    aggregate.aggregate_Data()