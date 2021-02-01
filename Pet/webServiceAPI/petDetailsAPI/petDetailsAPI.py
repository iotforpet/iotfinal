import json
import cherrypy_cors
import cherrypy
import webServiceAPI.petDetailsAPI.petData as sd
import requests
class SensorDataWebService(object):
    exposed = True

    def GET(self, *uri, **params):
        op1 = sd.MongoDbClient()
        res = None
        if str(uri[0]) == "getPetUserDetails" and len(uri) == 2:
            res = op1.getPetUserDetails(uri[1])
        elif str(uri[0]) == "getPetDetails" and len(uri) == 2:
            res = op1.getPetDetails(uri[1])
        else:
            return op1._formJson("Failed", "Wrong request")
        return (res)
    def POST(self, *uri):
        pass
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
        'server.socket_port': 9292,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(SensorDataWebService(), '/petdata', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()


