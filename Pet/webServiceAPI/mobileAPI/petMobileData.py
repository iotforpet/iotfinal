import json
from datetime import datetime, timedelta
from pymongo import MongoClient
class MongoDbClient(object):
    def __init__(self):
        self.mongoClient = MongoClient('localhost:27017')

        self.mongoDB = self.mongoClient["Pet_details"]

    def register(self, input):
        resexist = self.mongoDB["pet_owner_details"].find({"e_mail": input["e_mail"]})
        if(resexist.count()==0):
            ids = [{"userID":input['userID']}]
            mydict = {"e_mail":input['e_mail'],"password":input['password'],"IDS":ids,"username":input['username'],"type":"user"}
            res = self.mongoDB["pet_owner_details"].insert_one(mydict)
            if (res.acknowledged):
                out = json.dumps({'status': 200, 'Result': True, 'Output': "Registered successful"}).encode('utf8')
                return (out)
            else:
                return (self._formJson(400, False, "Registeration failed.Please try again later"))
        else:
            resexistid = self.mongoDB["pet_owner_details"].find({"$and": [{"e_mail": input["e_mail"]},
                                                                {"IDS": {"$elemMatch": {"userID":input['userID']}}}]},{"_id":0})
            if(resexistid.count()==0):
                ids = {"userID": input['userID']}
                res = self.mongoDB["pet_owner_details"].update_one({"e_mail": input["e_mail"]},
                                                                   { "$push":  { "IDS":ids}})
                if (res.acknowledged):
                    out = json.dumps({'status': 200, 'Result': True, 'Output': "Registered successful"}).encode('utf8')
                    return (out)
                else:
                    return (self._formJson(400, False, "Registeration failed.Please try again later"))
            else:
                return (self._formJson(400, False, "Registeration failed.Please try again later"))
        # else:
        #     return (self._formJson(400,False, "User already exist"))

    def resetPassword(self, input):
        resexist = self.mongoDB["pet_owner_details"].find(
            {"e_mail": input["e_mail"]})
        if (resexist.count() > 0):
            res = self.mongoDB["pet_owner_details"].update_one({ "e_mail":input["e_mail"]},
                                                               { "$set": {"password":input["confirm_password"]}})
            if (res.acknowledged):
                out = json.dumps({'status':200,'Result': True, 'Output': "Password changed successful"}).encode('utf8')
                return (out)
            else:
                return (self._formJson(400,False, "Password change failed.Please try again later"))
        else:
            return (self._formJson(400,False, "User does not exist"))

    def login(self,input):
        username  = input["username"]
        password = input["password"]
        res = self.mongoDB["pet_owner_details"].find({"e_mail": username, "password": password},{"_id": 0,"email":0,"password":0})
        if (res.count() > 0):
            out = json.dumps({'status':200,'Result': True, 'Output':  "Login successful",'userDetails':res.next()}).encode('utf8')
            return (out)
        else:
            return (self._formJson(400,False, "Login failed.Please check email and password"))

    def profile(self, input):
        res = self.mongoDB["pet_owner_details"].find({"e_mail":input["e_mail"]},
                                                     {"_id": 0, "email": 0, "password": 0})
        if (res.count() > 0):
            results = list(res)
            if (len(results) > 0):
                for idx, reset in enumerate(results[0]["IDS"]):
                    if reset["userID"] == input["userID"]:
                        print(reset)
                    else:
                        results[0]["IDS"].pop(idx)
            userDetails = results[0]
            out = json.dumps({'status':200,'Result': True, 'Output': "Profile fetched successfully", 'userDetails':userDetails}).encode('utf8')
            return (out)
        else:
            return (self._formJson(400,False, "Profile not found"))

    def getUser(self, input):
        resexist = self.mongoDB["pet_owner_details"].find( {"$and": [
        {"type": input['type']}
        # {"IDS.resource_catalog": {
        #     "$ne": None
        # }},
        #     {"IDS.deviceId": {
        #         "$ne": None
        #     }}
    ]},{"_id": 0,"IDS.userID":1,"e_mail":1,"username":1,"IDS.resource_catalog":1,"IDS.deviceId":1})
        if (resexist.count() > 0):
            out = json.dumps({'status': 200, 'Result': True, 'Output': list(resexist)}).encode('utf8')
            return (out)
        else:
            return (self._formJson(400, False, "Profile not found"))


    def adduser(self, input):
        resexist = self.mongoDB["pet_owner_details"].find({"$and": [{"e_mail": input["e_mail"]},
                                                                    {"IDS": {
                                                                        "$elemMatch": {"userID": input['userID']}}}]},
                                                          {"_id": 0})
        resexist1 = self.mongoDB["pet_owner_details"].aggregate([{"$match": {"e_mail": input["e_mail"]}},
                                                                 {"$project": {"matchedIndex": {
                                                                     "$indexOfArray": ["$IDS.userID",
                                                                                       input['userID']]}}}])
        # resexist = self.mongoDB["pet_owner_details"].find(
        #     {"$and": [{"userID": input["petID"]}]})
        if (resexist.count() > 0):
            r = list(resexist1)
            res = self.mongoDB["pet_owner_details"].update_one({"$and": [{"e_mail": input["e_mail"]},
                                                                      {"IDS": {
                                                                          "$elemMatch": {"userID": input['userID']}}}]},
                                                               {"$set": {"IDS."+str(r[0]["matchedIndex"])+".resource_catalog": input["resource_catalog"],
                                                                         "IDS."+str(r[0]["matchedIndex"])+".deviceId": input['deviceId']}})
            if (res.acknowledged):
                out = json.dumps({'status':200,'Result': True, 'Output': "Profile activted successful"}).encode('utf8')
                return (out)
            else:
                return (self._formJson(400,False, "Profile activation failed.Please try again later"))
        else:
            return (self._formJson(400,False, "Profile not found"))

    def removeuser(self, input):
        resexist = self.mongoDB["pet_owner_details"].find({"$and": [{"e_mail": input["e_mail"]},
                                                                    {"IDS": {
                                                                        "$elemMatch": {"userID": input['userID']}}}]},
                                                          {"_id": 0})
        resexist1 = self.mongoDB["pet_owner_details"].aggregate([{"$match": {"e_mail": input["e_mail"]}},
                                                                 {"$project": {"matchedIndex": {
                                                                     "$indexOfArray": ["$IDS.userID",
                                                                                       input['userID']]}}}])
        # resexist = self.mongoDB["pet_owner_details"].find(
        #     {"$and": [{"userID": input["petID"]}]})
        if (resexist.count() > 0):
            r = list(resexist1)
            res = self.mongoDB["pet_owner_details"].update_one({"$and": [{"e_mail": input["e_mail"]},
                                                                         {"IDS": {
                                                                             "$elemMatch": {
                                                                                 "userID": input['userID']}}}]},
                                                               {"$set": {"IDS." + str(
                                                                   r[0]["matchedIndex"]) + ".resource_catalog": None,
                                                                         "IDS." + str(
                                                                             r[0]["matchedIndex"]) + ".deviceId": None}})
            if (res.acknowledged):
                out = json.dumps({'status':200,'Result': True, 'Output': "Profile deactivted successful"}).encode('utf8')
                return (out)
            else:
                return (self._formJson(400,False, "Profile deactivation failed.Please try again later"))
        else:
            return (self._formJson(400,False, "Profile not found"))

    def editProfile(self, input):
        resexist = self.mongoDB["pet_owner_details"].find({"$and": [{"e_mail": input["e_mail"]},
                                                                      {"IDS": {
                                                                          "$elemMatch": {"userID": input['userID']}}}]},
                                                            {"_id": 0})
        resexist1 = self.mongoDB["pet_owner_details"].aggregate([{"$match": {"e_mail": input["e_mail"]}},
                    {"$project": {"matchedIndex": {"$indexOfArray": ["$IDS.userID", input['userID']]}}}])
        if (resexist.count() > 0):
            r = list(resexist1)
            print(r[0]["matchedIndex"])
            res = self.mongoDB["pet_owner_details"].update_one({"$and": [{"e_mail": input["e_mail"]},
                                                                      {"IDS": {
                                                                          "$elemMatch": {"userID": input['userID']}}}]},
                                                               {"$set": {"IDS."+str(r[0]["matchedIndex"])+".name_of_animal": input["name_of_animal"],
                                                                         "phone_number": input["phone_number"],
                                                                         "IDS."+str(r[0]["matchedIndex"])+".type_of_animal": input["type_of_animal"],
                                                                         "IDS."+str(r[0]["matchedIndex"])+".breed": input["breed"],
                                                                         "IDS."+str(r[0]["matchedIndex"])+".age_of_animal": input["age_of_animal"],
                                                                         "IDS."+str(r[0]["matchedIndex"])+".sex_of_animal": input["sex_of_animal"]}})
            if (res.acknowledged):
                out = json.dumps({'status':200,'Result': True, 'Output': "Profile edited successful"}).encode('utf8')
                return (out)
            else:
                return (self._formJson(400,False, "Profile editting failed.Please try again later"))
        else:
            return (self._formJson(400,False, "Profile not found"))

    def petAppoint(self, input):
        resexist = self.mongoDB["pet_owner_details"].find(
            {"e_mail": input["e_mail"]})
        if (resexist.count() > 0):
            mydict = {"e_mail": input['e_mail'], "typeOfService": input['typeOfService'], "customerVoice": input['customerVoice'],
                      "timeStamp": input['timeStamp']}
            res = self.mongoDB["pet_appointment"].insert_one(mydict)
            if (res.acknowledged):
                out = json.dumps({'status':200,'Result': True, 'Output': "Appointment fixed successful"}).encode('utf8')
                return (out)
            else:
                return (self._formJson(400,False, "Appointment not fixed.Please try again later"))
        else:
            return (self._formJson(400,False, "Profile not found. So Appointment not fixed"))
    def getAppoint(self, input):
        res = self.mongoDB["pet_appointment"].find({"e_mail":input},{"_id": 0}).sort([("timeStamp", -1)]).limit(2)
        return list(res)
    def _formJson(self,httpval, status, val):
        return (json.dumps({'status':httpval,'Result': status, 'Output': val})).encode('utf8')

