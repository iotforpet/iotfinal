import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util
import dateutil.parser as parser
from dateutil import rrule

class MongoDbClient(object):
    def __init__(self):
        self.mongoClient = MongoClient('localhost:27017')
        self.mongoDB = self.mongoClient["Pet_data"]

    def getCurrent (self, sensorId):
        res = self.mongoDB["sensorData"].find({"sensorID":sensorId},{"_id": 0,"date":0}).sort([("date", -1)]).limit(1)
        results = list(res)
        if(len(results)>0):
            return (self._formJson("success",results))
        else:
            return (self._formJson("failed", None))

    def getCurrentAll (self, sensorId):
        res = self.mongoDB["sensorData"].find({"$and": [{"sensorID":sensorId},{"Status": {"$ne":None}}]},{"_id": 0}).sort([("date", -1)]).limit(1)
        if(res.count()>0):
            return res.next()
        else:
            return None

    def updateStatus (self, time,status):
        date = parser.parse(time)
        res = self.mongoDB["sensorData"].update_one({"date":date},{'$set': {"Status": status}})
        if res.acknowledged:
            reg_jsonres = {'Result': "Success", 'Message': "Updated"}
        else:
            reg_jsonres = {'Result': "Failed", 'Message': "Updated Failed"}
        print(reg_jsonres)
        return (json.dumps(reg_jsonres)).encode('utf8')

    def insertSensorData(self, inputdata):

        if (inputdata != ""):
            inputdata["date"] = datetime.strptime(inputdata["date"], '%Y-%m-%dT%H:%M:%S.%f')
            print(inputdata)
            res = self.mongoDB["sensorData"].insert_one(inputdata);
            print(res.acknowledged)
            if res.acknowledged:
                reg_jsonres = {'Result': "Success", 'Message': "Inserted"}
            else:
                reg_jsonres = {'Result': "Failed", 'Message': "Insertion Failed"}
            print(reg_jsonres)
        else:
            reg_jsonres = {'Result': "Success", 'Message': "Nothing to Insert"}
        return (json.dumps(reg_jsonres)).encode('utf8')

    def getToday(self,sensor):
        hour = [0 for _ in range(24)]
        dt = datetime.combine(datetime.now().date(), datetime.min.time())
        filtercolumn = "sensorID"
        filterValue = sensor
        res = self.mongoDB["sensorData"].aggregate([
            {"$match": {'date': {"$gte": dt}
                , '{}'.format(filtercolumn): '{}'.format(filterValue)}}
            , {"$project": {'hour': {'$hour': '$date'}, '{}'.format("sensorData"): 1, '_id': 0}}
            , {"$group": {"_id": "$hour", "value": {"$avg": "${}".format("sensorData")}}}
            , {"$sort": {"_id": 1}}
        ])
        print(list(res))
        if res:
            for i, j in enumerate(res):
                hour[j["_id"]] = j['value']


        return (self._formJson("success", hour))

    def getWeek(self, sensor):

        filtercolumn = "sensorID"
        filterValue = sensor

        output = {}

        # weekstarts from Monday
        fromDate = datetime.combine(
            datetime.now().date() - timedelta(days=7)
            , datetime.min.time())
        for dt in rrule.rrule(rrule.DAILY,
                              dtstart=fromDate,
                              until=datetime.now()):
            output[dt.strftime('%d-%m-%Y')] = 0
        res = self.mongoDB["sensorData"].aggregate([
            {"$match": {'date':
                            {"$gte": fromDate,
                             "$lte": datetime.now()}
                , '{}'.format(filtercolumn): '{}'.format(filterValue)}}
            , {"$project": {'DOM': {'$dayOfMonth': '$date'},
                            'month': {'$month': '$date'},
                            'year': {'$year': '$date'},
                            '{}'.format("sensorData"): 1, '_id': 0}}
            , {"$group": {"_id": {'month': '$month',
                                  'DOM': '$DOM',
                                  'year': '$year'},
                          "value": {"$avg": "${}".format("sensorData")}}}
            , {"$sort": {"_id.month": 1, "_id.DOM": -1, "_id.year": 1}}
        ])

        if res:
            for i, j in enumerate(res):
                strDate = "{}-{}-{}".format(str(j["_id"]["DOM"]).zfill(2), str(j["_id"]["month"]).zfill(2),
                                            j["_id"]["year"])
                output[strDate] = j['value']
        output = sorted(output.items(), key=lambda x: datetime.strptime(x[0], '%d-%m-%Y'))
        return (self._formJson("success", output))

    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val}, default=json_util.default)).encode('utf8')

