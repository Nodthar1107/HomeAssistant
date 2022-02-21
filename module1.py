import paho.mqtt.client as mqtt
import json
import xmltodict

def send_to_mqtt(data, id):
    topic = ""
    for el in topics_list:
        if id in el:
            topic = el.split("/")
            break
    topic = "home/TestGeneratorService/" + topic[-2] + "/" + topic[-1]
    client.publish(topic, payload = data)

def find_state(id):
    for el in devices_unique_id:
        if id in el:
            state = el.split("-")[-1]
            return state

def on_connect(client, userdata, flags, rc):
    client.subscribe("homeassistant/#")

def on_message(client, userdata, msg):

    if "homeassistant" in msg.topic:
        data = json.loads(msg.payload)
        if data["state_topic"] not in topics_list:
            topics_list.append(data["state_topic"])
            devices_unique_id.append(data["unique_id"])
            client.subscribe(topics_list[-1])

            
            register_invalid_topics(data["device"]["identifiers"][0])
    else:
        if "Hass" in msg.topic:
            pass
        elif "Device" in msg.topic:
            dev_to_correct(msg.payload, msg.topic)
        elif "XmlSensor_" in msg.topic:
            xml_to_correct(msg.payload, msg.topic)
        elif "CSV-" in msg.topic:
            csv_to_correct(msg.payload, msg.topic)
        elif "Sensor" in msg.topic:
            text_to_correct(msg.payload, msg.topic)
        

        

    #if topic != None:
    #    topics_list.append(topic)
    #    client.subscribe(topic)
    #    print(topics_list)
    #else:
    #   print(topic)

def register_invalid_topics(identifier):
    client.subscribe("Device" + str(identifier))
    client.subscribe("Binary-" + str(identifier) + "-Sensor")
    client.subscribe("XmlSensor_" + str(identifier))
    client.subscribe("CSV-" + str(identifier))
    client.subscribe("Sensor" + str(identifier))


# data preformers

def dev_to_correct(invalid_data, topic):
    invalid_data = json.loads(invalid_data.decode("utf-8"))
    id = topic.split("vice")[1]
    state = find_state(id)
    

    valid_data = { 
        "Id": invalid_data["Sensor"],
        "name": invalid_data["Sensor"],
        state: invalid_data["value"]           
    }

    send_to_mqtt(json.dumps(valid_data), id)

def xml_to_correct(invalid_data, topic):
    invalid_data = json.loads(json.dumps(xmltodict.parse((str(invalid_data) + "</sensor>")[1:].replace('\'', ''))))
    identifier = topic.split("_")[1]

    valid_data = { 
        "Id": identifier,
        "name": identifier,
        invalid_data["sensor"]["data"]["name"]: invalid_data["sensor"]["data"]["value"]        
    }

    send_to_mqtt(json.dumps(valid_data), identifier)

def csv_to_correct(invalid_data, topic):
    invalid_data = invalid_data.decode("utf-8").split(";")
    identifier = topic.split("-")[1]
    valid_data = { 
        "Id": identifier,
        "name": identifier,
        invalid_data[0]: invalid_data[1]        
    }

    send_to_mqtt(json.dumps(valid_data), identifier)

def text_to_correct(invalid_data, topic):
    identifier = topic.split("r")[1]
    state = find_state(identifier)
    valid_data = { 
        "Id": identifier,
        "name": identifier,
        state: invalid_data.decode("utf-8")      
    }

    send_to_mqtt(json.dumps(valid_data), identifier)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username = "vlad", password = "for7566476ds")

devices_unique_id= []
topics_list = []

client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
