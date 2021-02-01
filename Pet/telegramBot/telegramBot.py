import json
import paho.mqtt.client as MQTT
from telegram import InlineKeyboardMarkup
from telegram import InlineKeyboardButton
from telegram.ext import Updater
from telegram.ext import CommandHandler
from telegram.ext import MessageHandler
from telegram.ext import Filters
from telegram.ext import CallbackQueryHandler
import logging
from datetime import datetime
import requests

telBotUsers = {}
# Token
token = '1217038480:AAE0Zp8YCHq2SNV6IeVwaZ4N2cXHPrSx5gg'

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def myOnConnect(paho_mqtt, userdata, flags, rc):
    print ("Connected to message broker with result code: " + str(rc))


def myOnMessageReceived(paho_mqtt, userdata, msg):
    if(msg.topic == "petService/centralController/telegram/messages"):
        broadcast(json.loads(msg.payload)['petID'], json.loads(msg.payload)['value'])

def initiateSystem(chatID):
    global telBotUsers
    systemFile = open("baseConfig.json")
    systemInfo = json.loads(systemFile.read())
    telBotUsers[chatID]["deviceID"] = systemInfo["deviceID"]
    telBotUsers[chatID]["catalogURL"] = systemInfo["catalogURL"]
    deviceID = telBotUsers[chatID]["deviceID"]
    catalogURL = telBotUsers[chatID]["catalogURL"]
    getInfo = json.loads(requests.get(catalogURL + "/getinfo/"+deviceID).text)
    if getInfo["Result"] == "success":
        info = getInfo["Output"]
        telBotUsers[chatID]["petID"] = info["PetID"]
        telBotUsers[chatID]["deviceURL"] = info["Devices"][deviceID]["DC"]
        telBotUsers[chatID]["serviceURL"] = info["Devices"][deviceID]["SC"]
        telBotUsers[chatID]["lastUpdates"].append(getInfo["Output"]["lastUpdate"])
    getAPIs(chatID)

def getAPIs(chatID):
    global telBotUsers
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    catalogURL = telBotUsers[chatID]["catalogURL"]
    getInfo = json.loads(requests.get(catalogURL + "/getsensorInfo/" + petID + '/' + deviceID).text)
    if getInfo["Result"] == "success":
        sensorIDs = getInfo["Output"]["Devices"][deviceID]["Sensors"]

    getTelInfo = json.loads(requests.get(catalogURL + "/gettelusers/" + petID).text)
    if getTelInfo["Result"] == "success":
        telBotUsers[chatID]["telUsers"] = getTelInfo["Output"]["Tel_Users"]
    devReq = {
        "call": "getDeviceName",
        "petID": petID,
        "deviceID": deviceID,
        "data": sensorIDs
    }
    devReqResp = json.loads(requests.post(telBotUsers[chatID]["deviceURL"], json.dumps(devReq)).text)

    if devReqResp["Result"] == "success":
        telBotUsers[chatID]["sensors"] = devReqResp["Output"]
    servReq = {
        "call": "getService",
        "petID": petID,
        "deviceID": deviceID,
        "data":  "DataCollection"
    }
    serviceResp = json.loads(requests.post(telBotUsers[chatID]["serviceURL"], json.dumps(servReq)).text)
    if serviceResp["Result"] == "success":
        telBotUsers[chatID]["sensorDataAPI"] = serviceResp["Output"]["DataCollection"]["DataInsertAPI"]
        telBotUsers[chatID]["lastUpdates"].append(serviceResp["Output"]["last_update"])
# bot functions
def start(update, context):
    global telBotUsers
    """Send Welcome message, get chat_id and store it in resource catalog"""
    update.message.reply_text('Welcome to Smart Pet Service , a bot to monitor your pets.')
    chat_id = update.message.chat_id
    update.message.reply_text('Your Chat ID:{}'.format(chat_id))
    update.message.reply_text("To get status updates please add your chat id in the mobile settings section")
    register(chat_id)
    telBotUsers[chat_id] = {"lastUpdates": [], "initial": True}


def help(update, context):
    """Send Help message to user"""

    text = 'This bot helps you monitor your pets easily on Telegram:\n\n' \
           'To get status updates register your chat id in the mobile\n' \
           'type /sensor check sensor status of pet\n' \
           'type /user_activation check sensor status of pet\n' \
           'type /current check current reading of pet\n' \
           'type /sensor_status check sensor working status\n' \
           'type /sensor_user check sensor activated by user\n' \
           'type /cur_temp check current weight of pet\n' \
           'type /cur_weight_1 check current weight of pet\n' \
           'type /cur_weight_2 check current food available for pet\n' \
           'type /cur_water_level check current water available for pet\n' \
           'type /stop to exit'

    update.message.reply_text(text)
    chat_id = update.message.chat_id
    telBotUsers[chat_id] = {"lastUpdates": [], "initial": True}

def button(update, context):
    query = update.callback_query
    text = query_reply(query.data)
    query.edit_message_text(text)


def unknown(update, context):
    help(update, context)


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


def stop(update, context):
    """Bye, delete chat_id"""
    chat_id = update.message.chat_id
    delete(chat_id)
    update.message.reply_text('Bye')


# helper functions
def register(_id):
    # register chat_id in resource datalog
    print("chat id: ", _id)


def delete(_id):
    # deregister chat_id in resource datalog
    print("chat id: ", _id)

def broadcast(petID, msg):
    systemFile = open("baseConfig.json")
    systemInfo = json.loads(systemFile.read())
    systemFile.close()
    getTelUsers = json.loads(requests.get(systemInfo["catalogURL"]
                                          + "/gettelusers/"
                                          + petID).text)
    if getTelUsers["Result"] == "success":
        telUsers = getTelUsers["Output"]["Tel_Users"]
    else:
        telUsers = []
    try:
        for i in telUsers:
            updater.bot.send_message(i, msg, 'Markdown')
    except:
        pass

def sensor_activein(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    telUsers = telBotUsers[chatID]["telUsers"]
    if chatID in telUsers:
        keyboard = [[InlineKeyboardButton("Temperature Sensor", callback_data='tempstatus:{}'.format(chatID)),
                     InlineKeyboardButton("Pet Weight Sensor", callback_data='weightstatus:{}'.format(chatID)),
                     InlineKeyboardButton("Food Left Sensor", callback_data='foodstatus:{}'.format(chatID)),
                     InlineKeyboardButton("Water Left Sensor", callback_data='waterstatus:{}'.format(chatID))]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text('Please choose the sensor:', reply_markup=reply_markup)
    else:
        update.message.reply_text("User chat id not registered")

def sensor_status(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    if chatID in telUsers:
        getInfo = json.loads(requests.get(catalogURL + "/getsensorInfo/" + petID + '/' + deviceID).text)
        if getInfo["Result"] == "success":
            sensorIDs = getInfo["Output"]["Devices"][deviceID]["Sensors"]
        devReq = {
            "call": "getDeviceStatus",
            "petID": petID,
            "deviceID": deviceID,
            "data": sensorIDs
        }
        devReqResp = json.loads(requests.post(telBotUsers[chatID]["deviceURL"], json.dumps(devReq)).text)
        x = devReqResp['Output']
        text = "Active: 1"+"\n"+"Not Active: 0"+"\n\n"+"Water Sensor :"+str(x['WL_1']) +"\n"+"Temperature Sensor :" + str(x['temp_1']) + "\n"+"Food Weight Sensor :" + str(x['W_1']) + "\n"+"Pet Weight Sensor :" + str(x['W_2'])
        update.message.reply_text(text)
    else:
        update.message.reply_text("User chat id not registered")

def sensor_activestatus(chatID,sensorId):
    catalogURL = telBotUsers[chatID]["catalogURL"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    getInfo = json.loads(requests.get(catalogURL + "/getsensorInfo/" + petID + '/' + deviceID).text)
    if getInfo["Result"] == "success":
        sensorIDs = getInfo["Output"]["Devices"][deviceID]["Sensors"]
    devReq = {
        "call": "getDeviceStatus",
        "petID": petID,
        "deviceID": deviceID,
        "data": sensorIDs
    }
    devReqResp = json.loads(requests.post(telBotUsers[chatID]["deviceURL"], json.dumps(devReq)).text)
    x = devReqResp['Output']
    if sensorId == "WL_1":
        return str(x['WL_1'])
    elif sensorId == "temp_1":
        return str(x['temp_1'])
    elif sensorId == "W_1":
        return str(x['W_1'])
    elif sensorId == "W_2":
        return str(x['W_2'])

def sensor_useractivein(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    telUsers = telBotUsers[chatID]["telUsers"]
    if chatID in telUsers:
        keyboard = [[InlineKeyboardButton("Temperature Sensor", callback_data='tempuserstatus:{}'.format(chatID)),
                     InlineKeyboardButton("Pet Weight Sensor", callback_data='weightuserstatus:{}'.format(chatID)),
                     InlineKeyboardButton("Food Left Sensor", callback_data='fooduserstatus:{}'.format(chatID)),
                     InlineKeyboardButton("Water Left Sensor", callback_data='wateruserstatus:{}'.format(chatID))]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text('Please choose the sensor:', reply_markup=reply_markup)
    else:
        update.message.reply_text("User chat id not registered")

def sensor_useractivestatus(chatID,sensorId):
    catalogURL = telBotUsers[chatID]["catalogURL"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    getInfo = json.loads(requests.get(catalogURL + "/getsensorInfo/" + petID + '/' + deviceID).text)
    if getInfo["Result"] == "success":
        sensorIDs = getInfo["Output"]["Devices"][deviceID]["Sensors"]
    devReq = {
        "call": "getDeviceWebStatus",
        "petID": petID,
        "deviceID": deviceID,
        "data": sensorIDs
    }
    devReqResp = json.loads(requests.post(telBotUsers[chatID]["deviceURL"], json.dumps(devReq)).text)
    x = devReqResp['Output']
    if sensorId == "WL_1":
        return str(x['WL_1'])
    elif sensorId == "temp_1":
        return str(x['temp_1'])
    elif sensorId == "W_1":
        return str(x['W_1'])
    elif sensorId == "W_2":
        return str(x['W_2'])

def sensor_user(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    if chatID in telUsers:
        getInfo = json.loads(requests.get(catalogURL + "/getsensorInfo/" + petID + '/' + deviceID).text)
        if getInfo["Result"] == "success":
            sensorIDs = getInfo["Output"]["Devices"][deviceID]["Sensors"]
        devReq = {
            "call": "getDeviceWebStatus",
            "petID": petID,
            "deviceID": deviceID,
            "data": sensorIDs
        }
        devReqResp = json.loads(requests.post(telBotUsers[chatID]["deviceURL"], json.dumps(devReq)).text)
        x = devReqResp['Output']
        text = "Active: 1"+"\n"+"Not Active: 0"+"\n\n"+"Water Sensor :"+str(x['WL_1']) +"\n"+"Temperature Sensor :" + str(x['temp_1']) + "\n"+"Food Weight Sensor :" + str(x['W_1']) + "\n"+"Pet Weight Sensor :" + str(x['W_2'])
        update.message.reply_text(text)
    else:
        update.message.reply_text("User chat id not registered")

def cur_temp(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    sensorDataAPI = telBotUsers[chatID]["sensorDataAPI"]
    if chatID in telUsers:
        getInfo = json.loads(requests.get(sensorDataAPI + "/getCurrent/" + "temp_1").text)
        if getInfo["Result"] == "success":
            data = getInfo["Output"][0]
            text = "Current Temperature : " + str(data["sensorData"]) + " " + str(data["unit"])
        else:
            text = "Temperature sensor is not active or no data found"
        update.message.reply_text(text)
    else:
        update.message.reply_text("User chat id not registered")

def cur_weight_1(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    sensorDataAPI = telBotUsers[chatID]["sensorDataAPI"]
    if chatID in telUsers:
        getInfo = json.loads(requests.get(sensorDataAPI + "/getCurrent/" + "W_1").text)
        if getInfo["Result"] == "success":
            data = getInfo["Output"][0]
            text = "Current Food Weight : " + str(data["sensorData"]) + " " + str(data["unit"])
        else:
            text = "Pet Weight sensor is not active or no data found"
        update.message.reply_text(text)
    else:
        update.message.reply_text("User chat id not registered")

def cur_weight_2(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    sensorDataAPI = telBotUsers[chatID]["sensorDataAPI"]
    if chatID in telUsers:
        getInfo = json.loads(requests.get(sensorDataAPI + "/getCurrent/" + "W_2").text)
        if getInfo["Result"] == "success":
            data = getInfo["Output"][0]
            text = "Current Pet Weight : " + str(data["sensorData"]) + " " + str(data["unit"])
        else:
            text = "Food Weight sensor is not active or no data found"
        update.message.reply_text(text)
    else:
        update.message.reply_text("User chat id not registered")

def cur_water_level(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    sensorDataAPI = telBotUsers[chatID]["sensorDataAPI"]
    if chatID in telUsers:
        getInfo = json.loads(requests.get(sensorDataAPI + "/getCurrent/" + "WL_1").text)
        if getInfo["Result"] == "success":
            data = getInfo["Output"][0]
            text = "Current Water level : " + str(data["sensorData"]) + " " + str(data["unit"])
        else:
            text = "Water level sensor is not active or no data found"
        update.message.reply_text(text)
    else:
        update.message.reply_text("User chat id not registered")

def current_reading(chatID,sensorId):
    sensorDataAPI = telBotUsers[chatID]["sensorDataAPI"]
    getInfo = json.loads(requests.get(sensorDataAPI + "/getCurrent/" + sensorId).text)
    if getInfo["Result"] == "success":
        data = getInfo["Output"][0]
        text = str(data["sensorData"]) + " " + str(data["unit"])
    else:
        text = "sensor is not active or no data found"
    return text

def current(update, context):
    global telBotUsers
    chatID = update.message.chat_id
    initiateSystem(chatID)
    catalogURL = telBotUsers[chatID]["catalogURL"]
    sensors = telBotUsers[chatID]["sensors"]
    telUsers = telBotUsers[chatID]["telUsers"]
    deviceID = telBotUsers[chatID]["deviceID"]
    petID = telBotUsers[chatID]["petID"]
    sensorDataAPI = telBotUsers[chatID]["sensorDataAPI"]
    if chatID in telUsers:
        keyboard = [[InlineKeyboardButton("Temperature", callback_data='temp:{}'.format(chatID)),
                     InlineKeyboardButton("Pet Weight", callback_data='weight:{}'.format(chatID)),
                     InlineKeyboardButton("Food Left", callback_data='food:{}'.format(chatID)),
                     InlineKeyboardButton("Water Left", callback_data='water:{}'.format(chatID))]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text('Please choose the sensor:', reply_markup=reply_markup)
    else:
        update.message.reply_text("User chat id not registered")

def query_reply(inputData):
    global telBotUsers
    s, chatID = inputData.split(":")[0], int(inputData.split(":")[1].encode())
    if s == 'temp':
        text = current_reading(chatID, "temp_1")
        text = "Current Temperature of Room : " + text
        return text
    elif s == 'food':
        text = current_reading(chatID, "W_1")
        text = "Current Food Level of Pet : " + text
        return text
    elif s == 'weight':
        text = current_reading(chatID, "W_2")
        text = "Current Weight of Pet : " + text
        return text
    elif s == 'water':
        text = current_reading(chatID,"WL_1")
        text = "Current Water level : " + text
        return text
    elif s == 'tempstatus':
        text = sensor_activestatus(chatID, "temp_1")
        if text == "1":
            text = "Temperature sensor is active and working"
        else:
            text = "Temperature sensor is not active and not working"
        return text
    elif s == 'weightstatus':
        text = sensor_activestatus(chatID, "W_1")
        if text == "1":
            text = "Weight sensor is active and working"
        else:
            text = "Weight sensor is not active and not working"
        return text
    elif s == 'foodstatus':
        text = sensor_activestatus(chatID, "W_2")
        if text == "1":
            text = "Weight sensor is active and working"
        else:
            text = "Weight sensor is not active and not working"
        return text
    elif s == 'waterstatus':
        text = sensor_activestatus(chatID, "WL_1")
        if text == "1":
            text = "Water level sensor is active and working"
        else:
            text = "Water level sensor is not active and not working"
        return text
    elif s == 'tempuserstatus':
        text = sensor_useractivestatus(chatID, "temp_1")
        if text == "1":
            text = "Temperature sensor is activated by user"
        else:
            text = "Temperature sensor is not activated by user"
        return text
    elif s == 'weightuserstatus':
        text = sensor_useractivestatus(chatID, "W_1")
        if text == "1":
            text = "Weight sensor is activated by user"
        else:
            text = "Weight sensor is not activated by user"
        return text
    elif s == 'fooduserstatus':
        text = sensor_useractivestatus(chatID, "W_2")
        if text == "1":
            text = "Weight sensor is activated by user"
        else:
            text = "Weight sensor is not activated by user"
        return text
    elif s == 'wateruserstatus':
        text = sensor_useractivestatus(chatID, "WL_1")
        if text == "1":
            text = "Water level sensor is activated by user"
        else:
            text = "Water level sensor is not activated by user"
        return text
def main():
    global updater
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    # Make sure to set use_context=True to use the new context based callbacks
    # Post version 12 this will no longer be necessary
    updater = Updater(token, use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help))
    dp.add_handler(CommandHandler("current", current))
    dp.add_handler(CommandHandler("sensor", sensor_activein))
    dp.add_handler(CommandHandler("user_activation", sensor_useractivein))
    dp.add_handler(CommandHandler("sensor_status", sensor_status))
    dp.add_handler(CommandHandler("sensor_user", sensor_user))
    dp.add_handler(CommandHandler("cur_temp", cur_temp))
    dp.add_handler(CommandHandler("cur_weight_1", cur_weight_1))
    dp.add_handler(CommandHandler("cur_weight_2", cur_weight_2))
    dp.add_handler(CommandHandler("cur_water_level", cur_water_level))
    dp.add_handler(CallbackQueryHandler(button))
    dp.add_handler(CommandHandler("stop", stop))

    # on unknown commands - return help message
    dp.add_handler(MessageHandler(Filters.command, unknown))

    # on noncommand i.e message - return help message
    dp.add_handler(MessageHandler(Filters.text, help))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()
def callback(client, userdata, message):
    print(str(message.payload.decode("utf-8")))



if __name__ == '__main__':
    try:
        _paho_mqtt = MQTT.Client("PetService_Monitor", False)
        _paho_mqtt.on_connect = myOnConnect
        _paho_mqtt.connect("mqtt.eclipseprojects.io", 1883)
        _paho_mqtt.on_message = myOnMessageReceived
        _paho_mqtt.subscribe('petService/centralController/telegram/messages', 2)
        _paho_mqtt.loop_start()

    except:
        print("Error connecting to MQTT Broker.")
        pass
    main()
