from datetime import datetime
import json
import cherrypy
import cherrypy_cors
from pymongo import MongoClient
import socket, struct, platform


class Resource_Catalog():

    def __init__(self):
        self.mongoClient = MongoClient('localhost:27017')
        self.mongoDB = self.mongoClient["Pet_base"]
    def get_ip_address(self, curOS):
        if curOS == "Linux":
            import fcntl
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            return socket.inet_ntoa(fcntl.ioctl(
                s.fileno(),
                0x8915,  # SIOCGIFADDR
                struct.pack('256s', "wlan0".encode('utf-8'))
            )[20:24])
        elif curOS == "Windows":
            return (socket.gethostbyname(socket.gethostname()))
        else:
            return ("127.0.0.1")

    def getLastUpdate(self, petID):
        val = self.mongoDB["resource_catalog"].find({"PetID": petID}, {"lastUpdate": 1, "_id": 0})
        return (self._formJson("success", val.next()))

    def lastUpdate(self, petID):
        updateTime = str(datetime.now()).rsplit(':', 1)[0]
        self.mongoDB["resource_catalog"].update_one(
            {"PetID": petID},
            {"$set": {"lastUpdate": updateTime}}
        )

    def getInfo(self, deviceID):
        val = self.mongoDB["resource_catalog"].find \
            ({"Devices.{}".format(deviceID): {"$exists": True}}
             , {"_id": 0})
        res = val.next()
        print(res)
        res["Devices"][deviceID]["DC"] = (res["Devices"][deviceID]["DC"].format(self.get_ip_address(platform.system())))
        res["Devices"][deviceID]["SC"] = (res["Devices"][deviceID]["SC"].format(self.get_ip_address(platform.system())))
        return (self._formJson("success", res))
    #  get  sensor keys
    def getSensorKeys(self, petID, sensorType):
        val = self.mongoDB["resource_catalog"].find({"PetID": petID},
                                           {"SensorKeys.{}".format(sensorType): 1, "_id": 0})
        res = val.next()
        return (self._formJson("success", res))

    #  get  sensor Info
    def getsensorInfo(self, petID, deviceID):
        val = self.mongoDB["resource_catalog"].find \
            ({"PetID": petID}
             , {"_id": 0, "Devices.{}.Sensors".format(deviceID): 1})
        return (self._formJson("success", val.next()))

    #  get telegram user
    def getTelegramUsers(self, petID):
        val = self.mongoDB["resource_catalog"].find({"PetID": petID}, {"Tel_Users": 1, "_id": 0})
        return (self._formJson("success", val.next()))

    #  add telegram user
    def addTelegramUser(self, petID, users):
        for i in users:
            res = self.mongoDB["resource_catalog"].update_one(
                {"PetID": petID},
                {"$push": {"Tel_Users": int(i)}}
            )
        if res.acknowledged:
            self.lastUpdate(petID)
            return (self._formJson("success", "Added telegram User"))
        else:
            return (self._formJson("failed", "fail to add user"))

    #  remove telegram user
    def removeTelegramUser(self, petID, users):
        for i in users:
            res = self.mongoDB["resource_catalog"].update_one(
                {"PetID": petID},
                {"$pull": {"Tel_Users": i}}
            )
        if res.acknowledged:
            self.lastUpdate(petID)
            return (self._formJson("success", "Deleted Telegram User"))
        else:
            return (self._formJson("failed", "Failed to Remove user"))

    #  remove Mobile user
    def removeMobileUser(self, petID, users):
        for i in users:
            res = self.mongoDB["resource_catalog"].update_one(
                {"PetID": petID},
                {"$pull": {"Mobile_Users": i}}
            )
        if res.acknowledged:
            self.lastUpdate(petID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "Failed to Remove user"))
    # add Mobile user
    def addMobileUser(self, petID, users):
        for i in users:
            res = self.mongoDB["resource_catalog"].update_one(
                {"PetID": petID},
                {"$push": {"Mobile_Users": i}}
            )
        if res.acknowledged:
            self.lastUpdate(petID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "fail to add user"))

    # add devices
    def addDevices(self, petID, data):

        addType = data["type"]
        devID = data["deviceID"]
        val = data["values"]
        print(val)
        if addType == "All" and val["DC"] and val["SC"] and val["Sensors"]:
            res = self.mongoDB["resource_catalog"].update_one(
                {"PetID": petID},
                {"$set": {"Devices.{}".format(devID): val}}
            )
        elif addType == "Sensors" and val:
            for i in val:
                res = self.mongoDB["resource_catalog"].update_one(
                    {"PetID": petID},
                    {"$push": {"Devices.{}.Sensors".format(devID): i}}
                )
        if res.acknowledged:
            self.lastUpdate(petID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "fail to add user"))

    # Json Convertion
    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val})).encode('utf8')

# Cherrpy methods
class ResourceCatalogWebService(object):
    exposed = True

    def GET(self, *uri, **params):

        cat = Resource_Catalog()
        res = None
        if str(uri[0]) == "getinfo" and len(uri) == 2:
            res = cat.getInfo(uri[1])
            print(res)
        elif str(uri[0]) == "getsensorInfo" and len(uri) == 3:
            res = cat.getsensorInfo(uri[1], uri[2])
        elif str(uri[0]) == "getlastupdate" and len(uri) == 2:
            res = cat.getLastUpdate(uri[1])
        elif str(uri[0]) == "getkeys" and len(uri) == 3:
            res = cat.getSensorKeys(uri[1], uri[2])
        elif str(uri[0]) == "gettelusers" and len(uri) == 2:
            res = cat.getTelegramUsers(uri[1])
        else:
            return cat._formJson("Failed", "Wrong request")

        return (res)

    def POST(self, *uri, **params):
        cat = Resource_Catalog()
        res = None
        inputtext = json.loads(cherrypy.request.body.read())
        method = inputtext["call"]
        petID = inputtext["PetID"]

        if method == "adddevices":
            res = cat.addDevices(petID, inputtext["data"])
        elif method == "addtelusers":
            res = cat.addTelegramUser(petID, inputtext["Users"])
        elif method == "remtelusers":
            res = cat.removeTelegramUser(petID, inputtext["Users"])
        elif method == "addmobileusers":
            res = cat.addMobileUser(petID, inputtext["Users"])
        elif method == "remmobileusers":
            res = cat.removeMobileUser(petID, inputtext["Users"])
        else:
            return cat._formJson("Failed", "Wrong request")

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
        'server.socket_port': 8181,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(ResourceCatalogWebService(), '/resource_catalog', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()