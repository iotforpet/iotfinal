import json
import cherrypy_cors
import cherrypy
import webServiceAPI.sensorDataAPI.sensorData as sd
# import sensorData as sd
import datetime
import dateutil.parser
class SensorDataWebService(object):
    exposed = True

    def GET(self, *uri, **params):
        op1 = sd.MongoDbClient()
        res = None
        if str(uri[0]) == "getCurrent" and len(uri) == 2:
            res = op1.getCurrent(uri[1])
        elif str(uri[0]) == "updateStatus" and len(uri) == 3:
            res = op1.updateStatus(uri[1],uri[2])
        elif str(uri[0]) == "gettoday":
            res = op1.getToday(uri[1])
        elif str(uri[0]) == "getWeek":
            res = op1.getWeek(uri[1])
        else:
            return op1._formJson("Failed", "Wrong request")
        return (res)
    def POST(self, *uri):
        op1 = sd.MongoDbClient()
        response = None
        res=[]
        inputtext = json.loads(cherrypy.request.body.read())
        try:
            if inputtext["call"] == "insert":
                print(inputtext["data"])
                response = op1.insertSensorData(inputtext["data"])
            if inputtext["call"] == "getCurrentAll":
                for i in inputtext["data"]:
                    out = op1.getCurrentAll(i)
                    out['date']=  datetime.datetime.strptime(str(out['date']), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
                    res.append(out)
                response = json.dumps({'Result': "Success", 'Output':res})
            else:
                response = json.dumps({'Result': "Failed", 'Output': "Wrong Request"})
        except:
            response = json.dumps({'Result': "Failed", 'Output': "Request Failed"})

        return (response.encode('utf8'))

    def PUT(self):
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
        }
    }
    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8284,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(SensorDataWebService(), '/sensordata', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()


