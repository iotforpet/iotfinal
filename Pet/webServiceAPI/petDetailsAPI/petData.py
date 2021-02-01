import json
from datetime import datetime, timedelta
from pymongo import MongoClient
class MongoDbClient(object):
    def __init__(self):
        self.mongoClient = MongoClient('localhost:27017')

        self.mongoDB = self.mongoClient["Pet_details"]
    def getPetUserDetails (self, Id):
        # res = self.mongoDB["pet_owner_details"].find({"userID":Id},{"_id": 0}).limit(1)
        # res = self.mongoDB["pet_owner_details"].find({"IDS.{}".format(Id+".userID"): Id}
        #                                             ,{"IDS.{}".format(Id):1,"_id":0,"username":1,"password":1,"e_mail" :1,"phone_number":1}).limit(1)
        res = self.mongoDB["pet_owner_details"].find({"IDS": {"$elemMatch": {"userID":Id}}},{"_id":0}).limit(1)
        results = list(res)
        if (len(results) > 0):
            for idx, reset in enumerate(results[0]["IDS"]):
               if reset["userID"] == Id:
                    print(reset)
               else:
                    results[0]["IDS"].pop(idx)
            return (self._formJson("success", results))
        else:
            return (self._formJson("failed", None))

    def getPetDetails(self, name):
        res = self.mongoDB["pet_data"].find({"breedName": name}, {"_id": 0}).limit(1)
        results = list(res)
        if (len(results) > 0):
            return (self._formJson("success", results))
        else:
            return (self._formJson("failed", None))


    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val})).encode('utf8')

