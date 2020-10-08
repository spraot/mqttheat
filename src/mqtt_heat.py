#!/usr/bin/env python3

import os
import sys
import datetime
import json
import yaml
from statistics import mean
import paho.mqtt.client as mqtt
import time
import threading
import logging
import atexit
from room_control import RoomControl, modes
from sensor import TempSensor

TEMP_COMMAND = 1
MODE_COMMAND = 2
ROOM_STATE = 3
SENSOR_MSG = 4

class MqttHeatControl():

    config_file = 'config.yml'
    topic_prefix = 'pi/heat'
    homeassistant_prefix = 'homeassistant'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    pump_topic = ''
    update_freq = 5*60

    default_room = {
        'md-icon': 'home-thermometer',
        'adjacent_rooms': [],
        'modes': modes[:]
    }

    mqtt_topic_map = {}
    rooms = {}
    sensors = {}

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()
        self.configure_sensors()

        #Construct map for fast indexing
        for room in self.rooms.values():
            self.mqtt_topic_map[room['mqtt_temp_command_topic']] = (TEMP_COMMAND, room)
            self.mqtt_topic_map[room['mqtt_mode_command_topic']] = (MODE_COMMAND, room)
            self.mqtt_topic_map[room['mqtt_state_topic']] = (ROOM_STATE, room)

        for topic, sensor in self.sensors.items():
            self.mqtt_topic_map[topic] = (SENSOR_MSG, sensor)

        logging.debug('room list: '+', '.join(self.rooms.keys()))
        logging.debug('sensor list: '+', '.join(self.sensors.keys()))
        logging.debug('subscribed topics list: '+', '.join(self.mqtt_topic_map.keys()))

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

         #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'rooms']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        self.pump_topic = config['pump']

        for id, room in self.rooms.items():
            room['id'] = id

            if 'name' not in room:
                room['name'] = room['id']

            for k, v in self.default_room.items():
                if not k in room:
                    room[k] = v

            try:
                room['adjacent_rooms'] = [self.rooms[adj] for adj in room['adjacent_rooms']]
            except KeyError as e:
                raise KeyError('Cannot load configuration: cannot find adjacent room {} for room {}'.format(e, room['name']))

            room['mqtt_config_topic'] = '{}/room/{}/config'.format(self.homeassistant_prefix, room['id'])
            room['mqtt_mode_command_topic'] = '{}/{}/mode/set'.format(self.topic_prefix, room['id'])
            room['mqtt_temp_command_topic'] = '{}/{}/temperature/set'.format(self.topic_prefix, room['id'])
            room['mqtt_state_topic'] = '{}/{}/state'.format(self.topic_prefix, room['id'])
            room['mqtt_availability_topic'] = '{}/{}/availability'.format(self.topic_prefix, room['id'])

            room['control'] = RoomControl(room['name'])

    def configure_sensors(self):
        for room in self.rooms.values():
            for sensor_topic in room['sensors']:
                if sensor_topic not in self.sensors:
                    self.sensors[sensor_topic] = TempSensor(sensor_topic)
                room['control'].sensors.append(self.sensors[sensor_topic])

    def configure_mqtt_for_room(self, room):
        room_configuration = {
            'name': room['name'],
            'mode_command_topic': room['mqtt_mode_command_topic'],
            'temperature_command_topic': room['mqtt_temp_command_topic'],
            'state_topic': room['mqtt_state_topic'],
            'availability_topic': room['mqtt_availability_topic'],
            'device': {'identifiers': room['id']},
            'modes': room['modes']
        }

        try:
            room_configuration['unique_id'] = room['unique_id']
        except KeyError:
            room_configuration['unique_id'] = room['id']

        try:
            room_configuration['icon'] = 'mdi:' + room['md-icon']
        except KeyError:
            pass

        json_conf = json.dumps(room_configuration)
        logging.debug('Broadcasting homeassistant configuration for room ' + room['name'] + ': ' + json_conf)
        self.mqttclient.publish(room['mqtt_config_topic'], payload=json_conf, qos=0, retain=True)

    def start(self):
        logging.info('starting')

        #MQTT startup
        logging.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logging.info('MQTT client started')

        logging.info('Starting main thread')
        self.main_thread = threading.Thread(name='main', target=self.main)
        self.main_thread.start()

        logging.info('started')

    def main(self):
        time.sleep(2)
        while True:
            start = datetime.datetime.now()
            
            for room in self.rooms.values():
                room['control'].update()

                try:
                    temp_str = '{:0.1f}'.format(room['control'].get_temp())
                except TypeError:
                    temp_str = 'None'

                self.mqtt_broadcast_state(room)

                if 'output_heat_topic' in room:
                    heating_level = room['control'].heating_level
                    logging.info('Room {}: setting heat level to {} (current temp is {})'.format(room['name'], heating_level, temp_str))
                    self.mqttclient.publish(room['output_heat_topic'], payload='{:0.0f}'.format(heating_level), qos=1, retain=False)

                if 'output_cool_topic' in room:
                    cooling_level = room['control'].cooling_level
                    logging.info('Room {}: setting cooling level to {} (current temp is {})'.format(room['name'], cooling_level, temp_str))
                    self.mqttclient.publish(room['output_cool_topic'], payload='{:0.0f}'.format(cooling_level), qos=1, retain=False)

            heating_levels = [r['control'].heating_level for r in self.rooms.values() if 'output_heat_topic' in r]
            pump_state = max(heating_levels) > 20 or mean(heating_levels) > 5
            logging.info('Setting pump state to {}'.format(pump_state))
            self.mqttclient.publish(self.pump_topic, payload='ON' if pump_state else 'OFF', qos=1, retain=False)          

            time.sleep(self.update_freq - (datetime.datetime.now() - start).total_seconds())

    def programend(self):
        logging.info('stopping')

        for room in self.rooms.values():
            if 'output_heat_topic' in room:
                self.mqttclient.publish(room['output_heat_topic'], payload=0, qos=0, retain=False)
            if 'output_cool_topic' in room:
                self.mqttclient.publish(room['output_cool_topic'], payload=0, qos=0, retain=False)
            self.mqtt_broadcast_room_availability(room, 'offline')
        self.mqttclient.publish(self.pump_topic, payload='OFF', qos=0, retain=False)   

        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info('MQTT client connected with result code '+str(rc))

        for room in self.rooms.values():
            #Configure MQTT for rooms
            self.configure_mqtt_for_room(room)

            #Broadcast current room state to MQTT for rooms
            self.mqtt_broadcast_room_availability(room, 'online')

            #Subsribe to MQTT room updates
            for topic in self.mqtt_topic_map.keys():
                self.mqttclient.subscribe(topic)

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            msg_obj = self.mqtt_topic_map[str(msg.topic)]

            if msg_obj[0] == ROOM_STATE and msg.retain:
                room = msg_obj[1]
                logging.debug('Received retained state from MQTT for room {}: {}'.format(room['name'], payload_as_string))
                room['control'].set_state(json.loads(payload_as_string))

            if msg_obj[0] == TEMP_COMMAND:
                room = msg_obj[1]
                logging.info('Updating set temp in room {} to {}'.format(room['name'], payload_as_string))
                room['control'].set_temp = float(payload_as_string)
                self.mqtt_broadcast_state(room)

            if msg_obj[0] == MODE_COMMAND:
                room = msg_obj[1]
                logging.info('Updating mode in room {} to {}'.format(room['name'], payload_as_string))
                room['control'].update_mode(payload_as_string)
                self.mqtt_broadcast_state(room)

            if msg_obj[0] == SENSOR_MSG:
                sensor = msg_obj[1]
                logging.info('Received MQTT message for sensor ' + sensor.name)
                sensor.update(json.loads(payload_as_string))

        except Exception as e:
            logging.error('Encountered error: '+str(e))

    def mqtt_broadcast_room_availability(self, room, value):
       logging.debug('Broadcasting MQTT message on topic: ' + room['mqtt_availability_topic'] + ', value: ' + value)
       self.mqttclient.publish(room['mqtt_availability_topic'], payload=value, qos=0, retain=True)

    def mqtt_broadcast_state(self, room):
        topic = room['mqtt_state_topic']
        state = json.dumps(room['control'].get_state())
        logging.debug('Broadcasting MQTT message on topic: ' + topic + ', value: ' + state)
        self.mqttclient.publish(topic, payload=state, qos=0, retain=True)

if __name__ == '__main__':
    mqttHeatControl =  MqttHeatControl()
    mqttHeatControl.start()
