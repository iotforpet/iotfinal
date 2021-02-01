import json
from datetime import datetime
import cherrypy_cors
import cherrypy
import socket, struct, platform


class ServicesCatalog():

    def __init__(self):
        self.servicefile = open("service_catalog.json")
        self.services = json.loads(self.servicefile.read())
        self.servicefile.close()

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

    # update service
    def updateService(self, petID, deviceID, k):

        try:
            for data in k:
                serviceType = data["service"]
                serviceInfo = data["properties"]
                res = self.services[petID][deviceID][serviceType].keys()

                allow = [1 for i in list(serviceInfo.keys())
                         if (i in res)]

                if len(serviceInfo.keys()) == sum(allow):
                    for j in serviceInfo.keys():
                        self.services[petID][deviceID][serviceType][j] = serviceInfo[j]
                        output = "service updated"
                        updateTime = str(datetime.now()).rsplit(':', 1)[0]
                        self.services[petID][deviceID]["last_update"] = updateTime

                else:
                    return (self._formJson("Failed", "Keys didnt match"))

                with open('service_catalog.json', 'w') as f:
                    f.write(json.dumps(self.services))
                    f.close()
            return (self._formJson("success", output))

        except:
            return (self._formJson("failed", "Failed to Add"))

    # get Service
    def getService(self, petID, deviceID, filterType):

        res = None
        col = {}
        try:
            if "str" in str(type(filterType)):
                res = self.services[petID][deviceID]
                res["DataCollection"]["DataInsertAPI"] \
                    = res["DataCollection"]["DataInsertAPI"].format(self.get_ip_address(platform.system()))

            elif "list" in str(type(filterType)):
                for i in filterType:
                    col[i] = self.services[petID][deviceID][i]
                    if i == "DataCollection":
                        col[i]["DataInsertAPI"] \
                            = col[i]["DataInsertAPI"].format(self.get_ip_address(platform.system()))
                res = col
        except:
            res = None

        if res:
            return (self._formJson("success", res))
        else:
            return (self._formJson("success", "device not found"))

    # Json Convertion
    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val})).encode('utf8')
#  cherrpy methods
class ServiceCatalogWebService(object):
    exposed = True

    def GET(self, *uri, **params):
        #        return (str(len(params)))
        ser = ServicesCatalog()
        try:
            petID = uri[0]
            deviceID = uri[1]
            filtertype = uri[2]
            response = ser.getService(petID, deviceID, filtertype)
            return (response)
        except:
            return (ser._formJson("failed", "exception occured"))

    def POST(self, *uri, **params):

        ser = ServicesCatalog()
        res = None
        inputtext = json.loads(cherrypy.request.body.read())
        method = inputtext["call"]
        petID = inputtext["petID"]
        deviceID = inputtext["deviceID"]
        if method == "getService":
            res = ser.getService(petID, deviceID, inputtext["data"])
        elif method == "updateService":
            res = ser.updateService(petID, deviceID, inputtext["data"])
        else:
            return ser._formJson("Failed", "Wrong request")

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
                [('Content-Type', 'application/json'), ]
            #              , ('Access-Control-Allow-Origin', '*'),("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE"))]
        }
    }
    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8283,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(ServiceCatalogWebService(), '/service_catalog', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()