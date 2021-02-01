import json
import cherrypy_cors
import cherrypy
import webServiceAPI.mobileAPI.petMobileData as sd
import requests
class SensorDataWebService(object):
    exposed = True

    def GET(self, *uri, **params):
        pass
    def POST(self, *uri):
        op1 = sd.MongoDbClient()
        res = None
        inputtext = json.loads(cherrypy.request.body.read())
        service = str(uri[0])
        if service == 'login':
            res = op1.login(inputtext)
        elif service == 'register':
            res = op1.register(inputtext)
        elif service == 'resetPassword':
            res = op1.resetPassword(inputtext)
        elif service == 'profile':
            res = op1.profile(inputtext)
        elif service == 'editProfile':
            res = op1.editProfile(inputtext)
        elif service == 'petAppoint':
            res = op1.petAppoint(inputtext)
        elif service == 'getinfo':
            out = json.loads(requests.get(inputtext['resource_catalog'] + "/getinfo/" + inputtext['deviceId']).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'gettelegram':
            out = json.loads(requests.get(inputtext['resource_catalog'] + "/gettelusers/" + inputtext['petID']).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'addtelegram':
            input = {
                "call": "addtelusers",
                "PetID": inputtext['petID'],
                "Users": inputtext['Users']
            }
            out = json.loads(requests.post(inputtext['resource_catalog'],json.dumps(input)).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'deletetelegram':
            input = {
                "call": "remtelusers",
                "PetID": inputtext['petID'],
                "Users": inputtext['Users']
            }
            out = json.loads(requests.post(inputtext['resource_catalog'], json.dumps(input)).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'adduser':
            res = op1.adduser(inputtext)
        elif service == 'removeuser':
            res = op1.removeuser(inputtext)
        elif service == 'getServiceInfo':
            out = json.loads(requests.get(inputtext['service_catalog'] + "/" + inputtext['petID']+"/" + inputtext['deviceId']+"/str").text)
            output = {'T_Sensors':out['Output']['T-Sensors']['Frequency'],
                      'W_Sensors':out['Output']['W-Sensors']['Frequency'],
                      'WL_Sensors':out['Output']['WL-Sensors']['Frequency']}
            return op1._formJson(200,True, output)
        elif service == 'updateServiceLimt':
            input = {
                "call": "updateService",
                "petID": inputtext['petID'],
                "deviceID":inputtext['deviceId'],
                "data": inputtext['data'],
            }
            out = json.loads(requests.post(inputtext['service_catalog'], json.dumps(input)).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'getDeviceWebStatus':
            input = {
                "call": "getDeviceWebStatus",
                "petID": inputtext['petID'],
                "deviceID": inputtext['deviceId'],
                "data": inputtext['data'],
            }
            out = json.loads(requests.post(inputtext['device_catalog'], json.dumps(input)).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'updateWebStatus':
            input = {
                "call": "updateWebDevices",
                "petID": inputtext['petID'],
                "deviceID": inputtext['deviceId'],
                "catalogURL": inputtext['catalogURL'],
                "data": inputtext['data'],
            }
            out = json.loads(requests.post(inputtext['device_catalog'], json.dumps(input)).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'getDeviceStatus':
            input = {
                "call": "getDeviceStatus",
                "petID": inputtext['petID'],
                "deviceID": inputtext['deviceId'],
                "data": inputtext['data'],
            }
            out = json.loads(requests.post(inputtext['device_catalog'], json.dumps(input)).text)
            return op1._formJson(200,True, out['Output'])
        elif service == 'getToday':
            serviceInfo = json.loads(requests.get(
                inputtext['service_catalog'] + "/" + inputtext['petID'] + "/" + inputtext['deviceId'] + "/str").text)
            sensorData = json.loads(
                requests.get(serviceInfo['Output']['DataCollection']['DataInsertAPI']+ "/gettoday/"+inputtext['sensorId']).text)
            labels =["12:00 AM", "1:00 AM", "2:00 AM", "3:00 AM", "4:00 AM", "5:00 AM", "6:00 AM", "7:00 AM", "8:00 AM",
                     "9:00 AM", "10:00 AM", "11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM",
                     "5:00 PM", "6:00 PM", "7:00 PM", "8:00 PM", "9:00 PM", "10:00 PM", "11:00 PM"]
            list=[]
            for idx, i in enumerate(sensorData['Output']):
                dict = {"datetime":labels[idx],"data":i}
                list.append(dict)
            return json.dumps(
                {'status':200,'Result': True,  'Output': list}).encode(
                'utf8')
        elif service == 'getWeek':
            serviceInfo = json.loads(requests.get(
                inputtext['service_catalog'] + "/" + inputtext['petID'] + "/" + inputtext['deviceId'] + "/str").text)
            sensorData = json.loads(
                requests.get(serviceInfo['Output']['DataCollection']['DataInsertAPI'] + "/getWeek/" + inputtext[
                    'sensorId']).text)
            list = []
            for i in sensorData['Output']:
                dict = {"datetime":i[0], "data": i[1]}
                list.append(dict)
            return json.dumps(
                {'status':200,'Result': True, 'Output': list}).encode(
                'utf8')
        elif service == 'getHome':
            input = {
                "call": "getDeviceStatus",
                "petID": inputtext['petID'],
                "deviceID": inputtext['deviceId'],
                "data": inputtext['data'],
            }
            sensorinput= {
                "call": "getCurrentAll",
                "petID": inputtext['petID'],
                "deviceId": inputtext['deviceId'],
                "data": inputtext['data'],
            }
            serviceInfo = json.loads(requests.get(
                inputtext['service_catalog'] + "/" + inputtext['petID'] + "/" + inputtext['deviceId'] + "/str").text)
            try:
                deviceStatus = json.loads(requests.post(inputtext['device_catalog'], json.dumps(input)).text)
            except:
                deviceStatus =None
            try:
                sensorData = json.loads(requests.post(serviceInfo['Output']['DataCollection']['DataInsertAPI'], json.dumps(sensorinput)).text)
            except:
                sensorData =None
            appoint = op1.getAppoint(inputtext['e_mail'])
            input = {
                "call": "getDeviceWebStatus",
                "petID": inputtext['petID'],
                "deviceID": inputtext['deviceId'],
                "data": inputtext['data'],
            }
            try:
                out = json.loads(requests.post(inputtext['device_catalog'], json.dumps(input)).text)
            except:
                out = None
            return json.dumps({'status':200,'Result': True, 'SensorStatus': deviceStatus,'UserSensorStatus': out,'Current':sensorData,'appoint':appoint}).encode('utf8')
        elif service == 'getUsers':
            res = op1.getUser(inputtext)
        else:
            return op1._formJson(400,"Failed", "Wrong request")
        return (res)
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
        'server.socket_port': 9293,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(SensorDataWebService(), '/petMobile', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()


