from datetime import datetime
import json
import cherrypy
import cherrypy_cors
import requests


class Device_Catalog():

    def __init__(self):
        self.devicefile = open("device_catalog.json")
        self.devices = json.loads(self.devicefile.read())
        self.devicefile.close()

#get Device info
    def getDevice(self, petID, deviceID, filterType, deviceFilter=''):

        res = None

        if filterType == "all":
            res = self.devices[petID][deviceID]

        elif filterType == "getlastupdate":
            res = self.devices[petID][deviceID]["last_update"]

        elif filterType == "sensors" and deviceFilter:
            try:
                res = self.devices[petID][deviceID][deviceFilter]
            except:
                res = "No such device type"


        elif filterType == "ID" and deviceFilter:
            res1 = self.devices[petID][deviceID]
            for i in res1.keys():
                for j in (res1[i]["installed{}".format(i)]):
                    if j["ID"] == deviceFilter:
                        res = j
        else:
            return (self._formJson("failed", None))

        if res:
            return (self._formJson("success", res))
        else:
            return (self._formJson("success", "device not found"))

# Json Convertion
    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val})).encode('utf8')

# get Device Web Status controlled by user
    def getDeviceWebStatus(self, petID, deviceID, sensorIDs):

        curSensors = []
        for i in self.devices[petID][deviceID].keys():
            if i != "last_update":
                for j in (self.devices[petID][deviceID][i] \
                        ["installed{}".format(i)]):
                    curSensors.append(j)
        res = {}
        for i in sensorIDs:
            for j in curSensors:
                if i == j["ID"]:
                    res[i] = j["web_active"]

        if res:
            return (self._formJson("success", res))
        else:
            return (self._formJson("success", "device not found"))

# get Device Names that is sensor in th kit
    def getDeviceName(self, petID, deviceID, sensorIDs):

        curSensors = []
        for i in self.devices[petID][deviceID].keys():
            if i != "last_update":
                for j in (self.devices[petID][deviceID][i] \
                        ["installed{}".format(i)]):
                    curSensors.append(j)
        res = {}
        for i in sensorIDs:
            for j in curSensors:
                if i == j["ID"]:
                    res[i] = j["Name"]

        if res:
            return (self._formJson("success", res))
        else:
            return (self._formJson("success", "device not found"))

# get Device Status - Sensor Working Status
    def getDeviceStatus(self, petID, deviceID, sensorIDs):

        curSensors = []
        for i in self.devices[petID][deviceID].keys():
            if i != "last_update":
                for j in (self.devices[petID][deviceID][i] \
                        ["installed{}".format(i)]):
                    curSensors.append(j)
        res = {}
        for i in sensorIDs:
            for j in curSensors:
                if i == j["ID"]:
                    res[i] = j["active"]

        if res:
            return (self._formJson("success", res))
        else:
            return (self._formJson("success", "device not found"))

    def getDeviceName(self, petID, deviceID, sensorIDs):

        curSensors = []
        for i in self.devices[petID][deviceID].keys():
            if i != "last_update":
                for j in (self.devices[petID][deviceID][i] \
                        ["installed{}".format(i)]):
                    curSensors.append(j)
        res = {}
        for i in sensorIDs:
            for j in curSensors:
                if i == j["ID"]:
                    res[i] = j["Name"]

        if res:
            return (self._formJson("success", res))
        else:
            return (self._formJson("success", "device not found"))


    def updateDevice(self, petID, deviceID, catalogURL, data):
        try:
            print(data)
            sensorType = data["sensor"]
            sensorID = data["sensorID"]
            deviceInfo = data["properties"]

            res = self.devices[petID][deviceID][sensorType] \
                ["installed{}".format(sensorType)]
            print(res)
            req = json.loads(
                requests.get(catalogURL + "/getkeys/" + petID + "/" + sensorType)
                    .text)
            if req["Result"] == "success":
                sensorKeys = req["Output"]["SensorKeys"][sensorType]

            allow = [1 for i in list(deviceInfo.keys())
                     if (i in sensorKeys[:-1])]

            nameCheck = 1
            for i in res:
                if i["Name"] == deviceInfo["Name"] and i["ID"] != sensorID:
                    nameCheck = 0

            if "ID" in deviceInfo.keys():
                return (self._formJson("failed", "can't Update Sensor ID"))
            print(sensorKeys, deviceInfo.keys(), allow, nameCheck)
            if len(deviceInfo.keys()) == sum(allow) and nameCheck:
                for i, j in enumerate(res):
                    if j["ID"] == sensorID:
                        for k in deviceInfo.keys():
                            self.devices[petID][deviceID][sensorType] \
                                ["installed{}".format(sensorType)][i][k] = deviceInfo[k]
                        output = "Device updated"
                        updateTime = str(datetime.now()).rsplit(':', 1)[0]
                        self.devices[petID][deviceID]["last_update"] = updateTime
                        self.devices[petID][deviceID][sensorType]["last_update"] = updateTime
                        break
                    else:
                        output = "Device not found"
            else:
                return (self._formJson("Failed", "Keys didnt match/Same name exists"))

            with open('device_catalog.json', 'w') as f:
                f.write(json.dumps(self.devices))
                f.close()
            return (self._formJson("success", output))

        except:
            return (self._formJson("failed", "Failed to Update"))

# update web status controlled by user to activate and deactivate sensor
    def updateWebDevice(self, petID, deviceID, catalogURL, data):
        try:
            sensorType = data["sensor"]
            sensorID = data["sensorID"]
            deviceInfo = data["properties"]

            res = self.devices[petID][deviceID][sensorType] \
                ["installed{}".format(sensorType)]
            req = json.loads(
                requests.get(catalogURL + "/getkeys/" + petID + "/" + sensorType)
                    .text)
            if req["Result"] == "success":
                sensorKeys = req["Output"]["SensorKeys"][sensorType]
            allow = [1 for i in list(deviceInfo.keys())
                     if (i in sensorKeys[:])]
            print(sensorKeys, deviceInfo.keys(), allow)
            if "ID" in deviceInfo.keys():
                return (self._formJson("failed", "can't Update Sensor ID"))
            print(sensorKeys, deviceInfo.keys(), allow)
            if len(deviceInfo.keys()) == sum(allow):
                for i, j in enumerate(res):
                    if j["ID"] == sensorID:
                        for k in deviceInfo.keys():
                            self.devices[petID][deviceID][sensorType] \
                                ["installed{}".format(sensorType)][i][k] = deviceInfo[k]
                        output = "Device updated"
                        updateTime = str(datetime.now()).rsplit(':', 1)[0]
                        self.devices[petID][deviceID]["last_update"] = updateTime
                        self.devices[petID][deviceID][sensorType]["last_update"] = updateTime
                        break
                    else:
                        output = "Device not found"
            else:
                return (self._formJson("Failed", "Keys didnt match/Same name exists"))

            with open('device_catalog.json', 'w') as f:
                f.write(json.dumps(self.devices))
                f.close()
            return (self._formJson("success", output))

        except:
            return (self._formJson("failed", "Failed to Update"))

# Cherrpy methods
class DeviceCatalogWebService(object):
    exposed = True

    def GET(self, *uri, **params):
        #        return (str(len(params)))
        dev = Device_Catalog()
        try:
            petID = uri[0]
            deviceID = uri[1]
            filtertype = uri[2]

            if filtertype == "all" or filtertype == "getlastupdate":
                response = dev.getDevice(petID, deviceID, filtertype)

            elif (filtertype == "ID" or filtertype == "sensors") and uri[3]:
                response = dev.getDevice(petID, deviceID, filtertype, uri[3])

            else:
                response = (dev._formJson("failed", "Not a valid request"))

            return (response)
        except:
            return (dev._formJson("failed", "exception occured"))

    def POST(self, *uri, **params):

        dev = Device_Catalog()
        res = None
        inputtext = json.loads(cherrypy.request.body.read())
        method = inputtext["call"]
        petID = inputtext["petID"]
        deviceID = inputtext["deviceID"]


        if method == "updateDevices":
            res = dev.updateDevice(petID, deviceID,
                                   inputtext["catalogURL"], inputtext["data"])
        elif method == "updateWebDevices":
            res = dev.updateWebDevice(petID, deviceID,
                                   inputtext["catalogURL"], inputtext["data"])
        elif method == "getDeviceName":
            res = dev.getDeviceName(petID, deviceID,
                                    inputtext["data"])
        elif method == "getDeviceStatus":
            res = dev.getDeviceStatus(petID, deviceID,
                                    inputtext["data"])
        elif method == "getDeviceWebStatus":
            res = dev.getDeviceWebStatus(petID, deviceID,
                                      inputtext["data"])

        else:
            return dev._formJson("Failed", "Wrong request")

        return (res)

    def PUT(self, *uri, **params):
        pass

    def DELETE(self):
        pass


def CORS():
    cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"

if __name__ == '__main__':
    cherrypy_cors.install()
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
			'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.CORS.on': True,
            'cors.expose.on': True,
            'tools.response_headers.headers':
            [('Content-Type', 'application/json'),]
        }
    }
    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8282,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(DeviceCatalogWebService(), '/device_catalog', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()