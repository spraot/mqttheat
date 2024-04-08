#!/usr/bin/env python3

from copy import deepcopy
from math import floor, pi, cos
import os
import sys
from datetime import datetime, timedelta
import json
import yaml
from statistics import mean
from threading import Event
import paho.mqtt.client as mqtt
import time
import signal
import threading
import logging
import atexit
from room_control import RoomControl
from sensor import Sensor
ROOM_TEMP_SET = 1
ROOM_MODE_SET = 2
ROOM_STATE = 3
ROOM_STATE_SET = 4
SENSOR_MSG = 5
CONFIG_SET = 6

class GracefulKiller:
  def __init__(self):
    self.kill_now = Event()
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now.set()

def hourInRange(hour, start, end):
    if end > 23:
        end = end - floor(end/24)*24
    if start <= end:
        return hour >= start and hour < end
    else:
        return hour >= start or hour < end

class MqttHeatControl():

    config_file = 'config.yml'
    topic_prefix = 'pi/heat'
    homeassistant_prefix = 'homeassistant'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    pump_topic = ''
    latitude = None
    longitude = None
    update_freq = 15*60
    _last_pump_cycle = None
    unique_id_suffix = '_mqttheat'
    history_hours = 12
    weather_today_topic = None
    weather_tomorrow_topic = None
    night_adjust_factor = 225
    keep_warm_modifier = 150
    keep_warm_ignore_cycles = 1
    night_modifier_peak_hour = 18
    keep_warm_threshold = 20
    uv_modifier_factor = 50

    config_options_mqtt = ['pump_topic', 'update_freq', 'latitude', 'longitude', 'night_adjust_factor', 'keep_warm_modifier', 'keep_warm_ignore_cycles', 'history_hours', 'night_modifier_peak_hour', 'keep_warm_threshold', 'uv_modifier_factor']
    config_options = [*config_options_mqtt, 'topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'rooms', 'unique_id_suffix', 'weather_today_topic', 'weather_tomorrow_topic']

    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.mqtt_topic_map = {}
        self.rooms = {}
        self.sensors = {}

        self.killer = GracefulKiller()

        self.weather_today = Sensor('weather_today')
        self.weather_tomorrow = Sensor('weather_tomorrow')

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()
        self.configure_sensors()
        self.make_all_room()

        for topic, sensor in self.sensors.items():
            self.mqtt_topic_map[topic] = (SENSOR_MSG, sensor)

        if self.weather_today_topic:
            self.mqtt_topic_map[self.weather_today_topic] = (SENSOR_MSG, self.weather_today)

        if self.weather_tomorrow_topic:
            self.mqtt_topic_map[self.weather_tomorrow_topic] = (SENSOR_MSG, self.weather_tomorrow)

        #Construct map for fast indexing
        for room in self.rooms.values():
            self.mqtt_topic_map[room['mqtt_set_state_topic']] = (ROOM_STATE_SET, room)
            self.mqtt_topic_map[room['mqtt_state_topic']] = (ROOM_STATE, room)
            self.mqtt_topic_map[room['mqtt_mode_command_topic']] = (ROOM_MODE_SET, room)
            self.mqtt_topic_map[room['mqtt_temp_command_topic']] = (ROOM_TEMP_SET, room)

        self.mqtt_topic_map['{}/config/set'.format(self.topic_prefix)] = (CONFIG_SET, None)

        logging.debug('room list: '+', '.join(self.rooms.keys()))
        logging.debug('sensor list: '+', '.join(self.sensors.keys()))
        logging.debug('subscribed topics list: '+', '.join(self.mqtt_topic_map.keys()))

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message
        self.mqttclient.will_set(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)

         #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            self.config = yaml.safe_load(f)

        for key in self.config_options:
            try:
                self.__setattr__(key, self.config[key])
            except KeyError:
                pass

        self.availability_topic = self.topic_prefix + '/bridge/state'

        self.rooms = deepcopy(self.rooms)
        for id, room in self.rooms.items():
            room['id'] = id

            if 'name' not in room:
                room['name'] = room['id']

            if not 'unique_id' in room:
                room['unique_id'] = room["id"].replace('/', '_')
            room['unique_id'] += self.unique_id_suffix

            if not 'adjacent_rooms' in room:
                room['adjacent_rooms'] = []
            try:
                room['adjacent_rooms'] = [self.rooms[adj] for adj in room['adjacent_rooms']]
            except KeyError as e:
                raise KeyError('Cannot load configuration: cannot find adjacent room {} for room {}'.format(e, room['name']))

            room['mqtt_config_topic'] = '{}/climate/{}/config'.format(self.homeassistant_prefix, room['unique_id'])
            room['mqtt_set_state_topic'] = '{}/{}/set'.format(self.topic_prefix, room['id'])
            room['mqtt_temp_command_topic'] = '{}/{}/temp/set'.format(self.topic_prefix, room['id'])
            room['mqtt_mode_command_topic'] = '{}/{}/mode/set'.format(self.topic_prefix, room['id'])
            room['mqtt_state_topic'] = '{}/{}'.format(self.topic_prefix, room['id'])
            room['mqtt_availability_topic'] = '{}/{}/availability'.format(self.topic_prefix, room['id'])
            if not 'can_heat' in room:
                room['can_heat'] = 'output_heat_topic' in room
            if not 'can_cool' in room:
                room['can_cool'] = 'output_cool_topic' in room

            room['control'] = RoomControl(room['name'], can_heat=room['can_heat'], can_cool=room['can_cool'])
            room['control'].set_state(room)
            room['heat_history'] = []

    def configure_sensors(self):
        for room in self.rooms.values():
            for sensor_topic in room['sensors']:
                if sensor_topic not in self.sensors:
                    self.sensors[sensor_topic] = Sensor(sensor_topic)
                room['control'].sensors.append(self.sensors[sensor_topic])

    def configure_mqtt_for_room(self, room):
        room_configuration = {
            'name': room['name'],
            'mode_command_topic': room['mqtt_mode_command_topic'],
            'temperature_command_topic': room['mqtt_temp_command_topic'],
            'json_attributes_topic': room['mqtt_state_topic'],
            'mode_state_topic': room['mqtt_state_topic'],
            'mode_state_template': '{{ value_json.mode }}',
            'temperature_state_topic': room['mqtt_state_topic'],
            'temperature_state_template': '{{ value_json.temperature }}',
            'current_temperature_topic': room['mqtt_state_topic'],
            'current_temperature_template': '{{ value_json.current_temperature }}',
            'temperature_unit': 'C',
            'temp_step': 0.1,
            'initial': 22.5,
            "availability": [
                {'topic': self.availability_topic, 'value_template': '{{ value_jason.state }}'},
                {'topic': room["mqtt_availability_topic"], 'value_template': '{{ value_jason.state }}'},
            ],
            "device": {
                "identifiers": [room["unique_id"]],
                "manufacturer": "KUNBUS GmbH",
                "model": "RevPi Digital IO",
                "name": room['name'],
                "sw_version": "mqttio"
            },
            'modes': room['control'].modes,
            "unique_id": room["unique_id"]
        }

        try:
            room_configuration['unique_id'] = room['unique_id']
        except KeyError:
            room_configuration['unique_id'] = room['id']

        json_conf = json.dumps(room_configuration)
        logging.debug('Broadcasting homeassistant configuration for room ' + room['name'] + ': ' + json_conf)
        self.mqttclient.publish(room['mqtt_config_topic'], payload=json_conf, qos=1, retain=True)

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
        self.killer.kill_now.wait(20)
        while not self.killer.kill_now.is_set():
            now = datetime.now()
            start = now

            logging.info(f'Updating heating/cooling levels for {len(self.rooms)} zones')
            
            base_pid_modifier_factor = 10
            base_pid_modifier = cos((now.hour - self.night_modifier_peak_hour)/24*2*pi) * base_pid_modifier_factor

            forecast = self.weather_today if now.hour < 6 else self.weather_tomorrow
            if forecast.is_connected():
                if base_pid_modifier > 0:
                    base_pid_modifier *= min(1, max(0.2, (12 - forecast.getValue('temperature_minimum')) / 18)) * self.night_adjust_factor / base_pid_modifier_factor

                base_pid_modifier -= forecast.getValue('ultraviolet_index_actual_average') * self.uv_modifier_factor * base_pid_modifier_factor

            logging.info('Base PID modifier: {}'.format(base_pid_modifier))

            history_len = round(self.history_hours*3600 / self.update_freq)

            for room in self.rooms.values():
                logging.debug(f'Updating room {room["name"]}')
                modifier_pid = base_pid_modifier

                # Apply keep warm modifier if the floor has been cold for a while
                heat_history = room['heat_history'][-history_len:-self.keep_warm_ignore_cycles]
                if len(heat_history) >= history_len-self.keep_warm_ignore_cycles and sum(heat_history) < self.keep_warm_threshold:
                    # If the average heating level over the last 'history_hours' hours is less than
                    # one cycle at 100%, let's increase the modifier to keep the floor warm
                    # Ignore N last cycles so that we get at least that many cycles of heating
                    modifier_pid += self.keep_warm_modifier
                    logging.debug(f'Applying keep warm modifier (+{self.keep_warm_modifier})')

                room['control'].update(modifier_pid=modifier_pid, modifier_onoff=-modifier_pid*0.005)

                try:
                    temp_str = '{:0.1f}'.format(room['control'].get_temperature())
                except TypeError:
                    temp_str = 'None'

                self.mqtt_broadcast_state(room)

                if room['control'].can_heat:
                    heating_level = room['control'].heating_level
                    logging.debug('Room {}: setting heat level to {} (current temp is {})'.format(room['name'], heating_level, temp_str))    
                else:
                    heating_level = 0
                if 'output_heat_topic' in room:
                    self.mqttclient.publish(room['output_heat_topic'], payload='{:0.0f}'.format(heating_level), qos=1, retain=True)

                if room['control'].can_cool:
                    cooling_level = room['control'].cooling_level
                    logging.debug('Room {}: setting cooling level to {} (current temp is {})'.format(room['name'], cooling_level, temp_str))
                else:
                    cooling_level = 0
                if 'output_cool_topic' in room:
                    self.mqttclient.publish(room['output_cool_topic'], payload='{:0.0f}'.format(cooling_level), qos=1, retain=True)

            heating_levels = [r['control'].heating_level for r in self.rooms.values() if 'output_heat_topic' in r]
            total_heating_level = sum(heating_levels)
            pump_state = total_heating_level >= 50
            self._set_pump_state(pump_state)
            logging.debug(f'Pump state: {pump_state} (total heating level: {total_heating_level})')

            self.mqtt_broadcast_state(self.room_all)

            for room in self.rooms.values():
                # Save heat history, including extra in case setting is changed
                # If pump is not running, no heating happens
                room['heat_history'].append(room['control'].heating_level / total_heating_level * 100 if pump_state else 0)
                if len(room['heat_history']) > history_len+self.keep_warm_ignore_cycles+50:
                    room['heat_history'].pop(0)

            # Cycle pump on daily basis
            if pump_state or not self._last_pump_cycle or self._last_pump_cycle < now - timedelta(days=1):
                if not pump_state:
                    logging.info('Heat water pump has been off for 24 hours, we\'ll run it for 30 seconds now')
                    self._set_pump_state(True)
                    self.killer.kill_now.wait(30)
                    self._set_pump_state(False)
                self._last_pump_cycle = datetime.now()

            self.killer.kill_now.wait(self.update_freq - (datetime.now() - start).total_seconds())

    def _set_pump_state(self, state):
        logging.debug('Setting pump state to {}'.format(state))
        self.mqttclient.publish(self.pump_topic, payload='ON' if state else 'OFF', qos=1, retain=True)

    def programend(self):
        logging.info('stopping')

        for room in self.rooms.values():
            if 'output_heat_topic' in room:
                self.mqttclient.publish(room['output_heat_topic'], payload=0, qos=1, retain=True)
            if 'output_cool_topic' in room:
                self.mqttclient.publish(room['output_cool_topic'], payload=0, qos=1, retain=True)
            self.mqtt_broadcast_room_availability(room, '')
        self.mqttclient.publish(self.pump_topic, payload='OFF', qos=1, retain=True)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)
        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info('MQTT client connected with result code '+str(rc))

        for room in self.rooms.values():
            #Configure MQTT for rooms
            self.configure_mqtt_for_room(room)

            #Broadcast current room state to MQTT for rooms
            self.mqtt_broadcast_room_availability(room, '{"state": "online"}')

            #Subsribe to MQTT room updates
            for topic in self.mqtt_topic_map.keys():
                self.mqttclient.subscribe(topic)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            msg_obj = self.mqtt_topic_map[str(msg.topic)]

            def set_state(room, state, do_broadcast=True):
                room = msg_obj[1]
                room['control'].set_state(state)
                if do_broadcast:
                    self.mqtt_broadcast_state(room)

            if msg_obj[0] == ROOM_MODE_SET:
                logging.debug('Received mode command from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], {'mode': payload_as_string})
                
            if msg_obj[0] == ROOM_TEMP_SET:
                logging.info('Received temperature command from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], {'set_temperature': float(payload_as_string)})

            if msg_obj[0] == ROOM_STATE_SET:
                logging.info('Received state from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], json.loads(payload_as_string))

            if msg_obj[0] == ROOM_STATE and msg.retain:
                logging.info('Received retained state from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], json.loads(payload_as_string), False)

            if msg_obj[0] == SENSOR_MSG:
                sensor = msg_obj[1]
                logging.debug('Received MQTT message for sensor ' + sensor.name)
                sensor.update(json.loads(payload_as_string))

            if msg_obj[0] == CONFIG_SET:
                config = json.loads(payload_as_string)
                for key in config:
                    if key not in self.config_options_mqtt:
                        raise KeyError('Cannot set config option: {}'.format(key))
                    if not (isinstance(config[key], str) or isinstance(config[key], int) or isinstance(config[key], float)):
                        raise ValueError('Cannot set config option: {} (invalid type)'.format(key))
                for key in config:
                    logging.info('Setting config option: {} = {}'.format(key, config[key]))
                    self.__setattr__(key, config[key])
                    self.config[key] = config[key]

                yaml.dump(self.config, open(self.config_file, 'w'), default_flow_style=False)

        except Exception as e:
            logging.error('Encountered error: '+str(e))

    def mqtt_broadcast_room_availability(self, room, value):
       logging.debug('Broadcasting MQTT message on topic: ' + room['mqtt_availability_topic'] + ', value: ' + value)
       self.mqttclient.publish(room['mqtt_availability_topic'], payload=value, qos=1, retain=True)

    def mqtt_broadcast_state(self, room):
        topic = room['mqtt_state_topic']
        state = json.dumps(room['control'].get_state())
        logging.debug('Broadcasting MQTT message on topic: ' + topic + ', value: ' + state)
        self.mqttclient.publish(topic, payload=state, qos=1, retain=room != self.room_all)

    def make_all_room(self):
        class AllRooms():
            def get_state(self2):
                state = None
                for room in self.rooms.values():
                    s = room['control'].get_state()
                    if not state:
                        state = s
                    else:
                        for k,v in list(state.items()):
                            if v != s[k]:
                                state.pop(k)
                return state
            
            def set_state(self2, state):
                for room in self.rooms.values():
                    room['control'].set_state(state)
                    self.mqtt_broadcast_state(room)

        self.room_all = {
            'name': 'all',
            'mqtt_state_topic': '{}/{}'.format(self.topic_prefix, 'all'),
            'control': AllRooms()
        }
        self.mqtt_topic_map['{}/{}/set'.format(self.topic_prefix, 'all')] = (ROOM_STATE_SET, self.room_all)
        self.mqtt_topic_map['{}/{}/mode/set'.format(self.topic_prefix, 'all')] = (ROOM_MODE_SET, self.room_all)
        self.mqtt_topic_map['{}/{}/temp/set'.format(self.topic_prefix, 'all')] = (ROOM_TEMP_SET, self.room_all)

if __name__ == '__main__':
    mqttHeatControl =  MqttHeatControl()
    mqttHeatControl.start()
