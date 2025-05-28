#!/usr/bin/env python3

from copy import deepcopy
from math import floor, pi, cos
import os
import sys
from datetime import datetime, timedelta
import json
import yaml
from threading import Event
import paho.mqtt.client as mqtt
import time
import signal
import threading
import logging
from pythonjsonlogger import jsonlogger
import atexit
from room_control import RoomControl
from sensor import Sensor

logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(message)%(levelname)', timestamp='dt')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO'))

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
    
def sat(value, min_value, max_value):
    if min_value is not None and max_value is not None and min_value > max_value:
        raise ValueError('min value larger than max value')
    if value is None:
        return None
    if min_value is not None and value < min_value:
        return min_value
    if max_value is not None and value > max_value:
        return max_value
    return value

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
    keep_warm_history_hours = 12
    recent_heat_offset_history_hours = 12
    weather_today_topic = None
    weather_tomorrow_topic = None
    night_adjust_factor = 150
    keep_warm_modifier = 275
    keep_warm_ignore_cycles = 1
    night_modifier_peak_hour = 18
    night_modifier_peak_width = 16  # hours
    keep_warm_threshold = 20
    uv_modifier_factor = 500
    recent_heat_offset_factor = 25

    config_options_mqtt = ['pump_topic', 'update_freq', 'latitude', 'longitude', 'night_adjust_factor', 'keep_warm_modifier', 'keep_warm_ignore_cycles', 'keep_warm_history_hours', 'recent_heat_offset_history_hours', 'recent_heat_offset_factor', 'night_modifier_peak_hour', 'night_modifier_peak_width', 'keep_warm_threshold', 'uv_modifier_factor']
    config_options = [*config_options_mqtt, 'topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'rooms', 'unique_id_suffix', 'weather_today_topic', 'weather_tomorrow_topic']

    def __init__(self):
        logger.info('Init')

        self.mqtt_topic_map = {}
        self.rooms = {}
        self.sensors = {}

        self.killer = GracefulKiller()

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()
        self.configure_sensors()
        self.make_all_room()

        #Construct topic map for fast indexing
        for topic, sensor in self.sensors.items():
            self.mqtt_topic_map[topic] = (SENSOR_MSG, sensor)

        for room in self.rooms.values():
            self.mqtt_topic_map[room['mqtt_set_state_topic']] = (ROOM_STATE_SET, room)
            self.mqtt_topic_map[room['mqtt_state_topic']] = (ROOM_STATE, room)
            self.mqtt_topic_map[room['mqtt_mode_command_topic']] = (ROOM_MODE_SET, room)
            self.mqtt_topic_map[room['mqtt_temp_command_topic']] = (ROOM_TEMP_SET, room)

        self.mqtt_topic_map['{}/config/set'.format(self.topic_prefix)] = (CONFIG_SET, None)

        logger.debug('room list: '+', '.join(self.rooms.keys()))
        logger.debug('sensor list: '+', '.join(self.sensors.keys()))
        logger.debug('subscribed topics list: '+', '.join(self.mqtt_topic_map.keys()))

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message
        self.mqttclient.will_set(self.availability_topic, payload='{"state": "offline"}', qos=1, retain=True)

         #Register program end event
        atexit.register(self.programend)

        logger.info('init done')

    def load_config(self):
        logger.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            self.config = yaml.safe_load(f)

        for key in self.config_options:
            try:
                self.__setattr__(key, self.config[key])
            except KeyError:
                pass

        self.availability_topic = self.topic_prefix + '/bridge/state'
        self.pump_state_topic = self.topic_prefix + '/pump'

        if self.night_modifier_peak_hour < 0 or self.night_modifier_peak_hour >= 24:
            raise ValueError('Invalid night_modifier_peak_hour: {}'.format(self.night_modifier_peak_hour))
        
        if self.night_modifier_peak_width <= 0:
            raise ValueError('Invalid night_modifier_peak_width, must be >0')

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
        self.weather_today = Sensor('weather_today')
        self.weather_tomorrow = Sensor('weather_tomorrow')

        if self.weather_today_topic:
            self.sensors[self.weather_today_topic] = self.weather_today

        if self.weather_tomorrow_topic:
            self.sensors[self.weather_tomorrow_topic] = self.weather_tomorrow

        for room in self.rooms.values():
            for sensor_topic in room['sensors']:
                if sensor_topic not in self.sensors:
                    self.sensors[sensor_topic] = Sensor(sensor_topic)
                room['control'].sensors.append(self.sensors[sensor_topic])

            if 'door_sensor_topic' in room:
                if room['door_sensor_topic'] not in self.sensors:
                    self.sensors[room['door_sensor_topic']] = Sensor(room['door_sensor_topic'])
                room['control'].door_sensor = self.sensors[room['door_sensor_topic']]

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
        logger.debug('Broadcasting homeassistant configuration for room ' + room['name'] + ': ' + json_conf)
        self.mqttclient.publish(room['mqtt_config_topic'], payload=json_conf, qos=1, retain=True)

    def start(self):
        logger.info('starting')

        #MQTT startup
        logger.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logger.info('MQTT client started')

        logger.info('Starting main thread')
        self.main_thread = threading.Thread(name='main', target=self.main)
        self.main_thread.start()

        logger.info('started')

    def main(self):
        temp_factor_cold_cutoff = 8
        temp_factor_hot_cutoff = 8
        temp_factor_slope = 1/12
        wind_factor_min = 1
        wind_factor_zero = 3
        wind_factor_slope = 1/17
        night_weather_adjust_min = 0.2
        night_weather_adjust_max = 1.2
        pump_output_ramp = 4
        pump_max_duty_cycle = 15*60  # seconds
        minimum_pump_duty_cycle = 30  # seconds
        minimum_pump_level = sat(minimum_pump_duty_cycle / self.update_freq * 100, 0.1, 100)
        use_tomorrows_forecast_cutoff_hour = 12

        self.killer.kill_now.wait(20)
        while not self.killer.kill_now.is_set():
            now = datetime.now()
            start = now

            logger.info(f'Updating heating/cooling levels for {len(self.rooms)} zones')
            
            peak_hour = (now.hour - self.night_modifier_peak_hour)/self.night_modifier_peak_width
            peak_hour += (
                + (2 if peak_hour < -1 else 0)
                - (2 if peak_hour > 1 else 0)
            )
            if -1/2 < peak_hour < 1/2:
                base_pid_modifier = sat(cos(peak_hour*pi), 0, None) * self.night_adjust_factor
            else:
                base_pid_modifier = 0

            forecast = self.weather_today if now.hour <= use_tomorrows_forecast_cutoff_hour else self.weather_tomorrow
            if forecast.is_connected():
                temp_factor = temp_factor_slope * (temp_factor_cold_cutoff - forecast.getValue('temperature_minimum'))
                wind_factor = wind_factor_min + wind_factor_slope * sat((forecast.getValue('wind_speed_max')-wind_factor_zero), 0, None)
                base_pid_modifier *= sat(temp_factor*wind_factor, night_weather_adjust_min, night_weather_adjust_max)
                base_pid_modifier += -forecast.getValue('ultraviolet_index_actual_average') * self.uv_modifier_factor

                base_pid_modifier -= sat(temp_factor_slope * (forecast.getValue('temperature_maximum') - temp_factor_hot_cutoff) * self.night_adjust_factor, 0, None)

            logger.info(f'Base PID modifier: {base_pid_modifier:.0f}')

            keep_warm_history_len = round(self.keep_warm_history_hours*3600 / self.update_freq)
            offset_history_len = round(self.recent_heat_offset_history_hours*3600 / self.update_freq)

            for room in self.rooms.values():
                logger.debug(f'Updating room {room["name"]}')
                modifier_pid = base_pid_modifier

                # Apply keep warm modifier if the floor has been cold for a while
                heat_history = room['heat_history'][-keep_warm_history_len:-self.keep_warm_ignore_cycles]
                if len(heat_history) >= keep_warm_history_len-self.keep_warm_ignore_cycles and sum(heat_history) < self.keep_warm_threshold:
                    # If the average heating level over the last 'keep_warm_history_hours' hours is less than
                    # one cycle at 100%, let's increase the modifier to keep the floor warm
                    # Ignore N last cycles so that we get at least that many cycles of heating
                    modifier_pid += self.keep_warm_modifier
                    logger.debug(f'Applying keep warm modifier (+{self.keep_warm_modifier})')

                # Adjustment for heat already in the floor, but not disappated yet
                heat_history = [0] * (offset_history_len - len(room['heat_history'])) + room['heat_history'][-offset_history_len:]
                if len(heat_history):
                    adj = sum((i+1) * x for i, x in enumerate(heat_history)) * self.recent_heat_offset_factor / offset_history_len / offset_history_len
                    modifier_pid -= adj
                    logger.debug(f'Applying adjustment for recent heating: ${adj:.0f}')

                modifier_pid *= room['modifier_factor'] if 'modifier_factor' in room else 1

                room['control'].update(modifier_pid=modifier_pid, modifier_onoff=-modifier_pid*0.005)

                try:
                    temp_str = '{:0.1f}'.format(room['control'].get_temperature())
                except TypeError:
                    temp_str = 'None'

                self.mqtt_broadcast_state(room)

                if room['control'].can_heat:
                    heating_level = room['control'].heating_level
                    logger.debug('Room {}: setting heat level to {:0.0f} (current temp is {})'.format(room['name'], heating_level, temp_str))    
                else:
                    heating_level = 0
                if 'output_heat_topic' in room:
                    self.mqttclient.publish(room['output_heat_topic'], payload='{:0.0f}'.format(heating_level), qos=1, retain=True)

                if room['control'].can_cool:
                    cooling_level = room['control'].cooling_level
                    logger.debug('Room {}: setting cooling level to {:0.0f} (current temp is {})'.format(room['name'], cooling_level, temp_str))
                else:
                    cooling_level = 0
                if 'output_cool_topic' in room:
                    self.mqttclient.publish(room['output_cool_topic'], payload='{:0.0f}'.format(cooling_level), qos=1, retain=True)

            radiant_heat_rooms = [r for r in self.rooms.values() if not r['name'].endswith('_ac')]
            heating_levels = [r['control'].heating_level for r in radiant_heat_rooms if 'output_heat_topic' in r]
            total_heating_level = sum(heating_levels)
            pump_level = sat(pump_output_ramp * total_heating_level / len(radiant_heat_rooms), 0, 100)
            logger.info(f'Pump level: {pump_level:.0f}%, total heating level: {total_heating_level:.0f}')

            self.mqtt_broadcast_state(self.room_all)

            # Cycle pump on daily basis
            if pump_level < minimum_pump_level and (not self._last_pump_cycle or self._last_pump_cycle < now - timedelta(days=1)):
                logger.info(f'Heat water pump has been off for 24 hours, we\'ll run it for {minimum_pump_duty_cycle} seconds now')
                pump_level = minimum_pump_level

            for room in self.rooms.values():
                # Save heat history, including extra in case setting is changed
                # If pump is not running, no heating happens
                room['heat_history'].append(room['control'].heating_level / total_heating_level * pump_level if total_heating_level else 0)
                history_len = max(keep_warm_history_len+self.keep_warm_ignore_cycles, offset_history_len)
                if len(room['heat_history']) > history_len+50:
                    room['heat_history'].pop(0)

            def seconds_left_in_cycle():
                return self.update_freq - (datetime.now() - start).total_seconds()

            self.mqttclient.publish(self.pump_state_topic, json.dumps({'pump_level': pump_level}))

            if pump_level >= minimum_pump_level:
                self._last_pump_cycle = datetime.now()
                seconds = sat(seconds_left_in_cycle(), 0, pump_max_duty_cycle)
                if seconds >= minimum_pump_duty_cycle and pump_level < 95:
                    while seconds >= minimum_pump_duty_cycle and not self.killer.kill_now.is_set():
                        self._set_pump_state(True)
                        self.killer.kill_now.wait(seconds*pump_level*0.01)
                        self._set_pump_state(False)
                        self.killer.kill_now.wait(seconds*(1-pump_level*0.01))
                        seconds = sat(seconds_left_in_cycle(), 0, pump_max_duty_cycle)
                else:
                    self._set_pump_state(True)
            else:
                self._set_pump_state(False)

            self.killer.kill_now.wait(seconds_left_in_cycle())

    def _set_pump_state(self, state):
        logger.debug('Setting pump state to {}'.format(state))
        self.mqttclient.publish(self.pump_topic, payload='ON' if state else 'OFF', qos=1, retain=True)

    def programend(self):
        logger.info('stopping')

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
        logger.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logger.info('MQTT client connected with result code '+str(rc))

        for room in self.rooms.values():
            #Configure MQTT for rooms
            self.configure_mqtt_for_room(room)

            #Broadcast current room state to MQTT for rooms
            self.mqtt_broadcast_room_availability(room, '{"state": "online"}')

            #Subsribe to MQTT room updates
            for topic in self.mqtt_topic_map.keys():
                self.mqttclient.subscribe(topic, qos=2)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=1, retain=True)

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logger.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            msg_obj = self.mqtt_topic_map[str(msg.topic)]

            def set_state(room, state, do_broadcast=True):
                room = msg_obj[1]
                room['control'].set_state(state)
                if do_broadcast:
                    self.mqtt_broadcast_state(room)

            if msg_obj[0] == ROOM_MODE_SET:
                logger.debug('Received mode command from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], {'mode': payload_as_string})
                
            if msg_obj[0] == ROOM_TEMP_SET:
                logger.info('Received temperature command from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], {'temperature': payload_as_string})

            if msg_obj[0] == ROOM_STATE_SET:
                logger.info('Received state from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], json.loads(payload_as_string))

            if msg_obj[0] == ROOM_STATE and msg.retain:
                logger.info('Received retained state from MQTT for room {}: {}'.format(msg_obj[1]['name'], payload_as_string))
                set_state(msg_obj[1], json.loads(payload_as_string), False)

            if msg_obj[0] == SENSOR_MSG:
                sensor = msg_obj[1]
                logger.debug('Received MQTT message for sensor ' + sensor.name)
                sensor.update(json.loads(payload_as_string))
                for room in self.rooms.values():
                    if sensor in room['control'].sensors:
                        self.mqtt_broadcast_state(room)

            if msg_obj[0] == CONFIG_SET:
                config = json.loads(payload_as_string)
                for key in config:
                    if key not in self.config_options_mqtt:
                        raise KeyError('Cannot set config option: {}'.format(key))
                    if not (isinstance(config[key], str) or isinstance(config[key], int) or isinstance(config[key], float)):
                        raise ValueError('Cannot set config option: {} (invalid type)'.format(key))
                for key in config:
                    logger.info('Setting config option: {} = {}'.format(key, config[key]))
                    self.__setattr__(key, config[key])
                    self.config[key] = config[key]

                yaml.dump(self.config, open(self.config_file, 'w'), default_flow_style=False)

        except Exception as e:
            logger.error('Encountered error: '+str(e))

    def mqtt_broadcast_room_availability(self, room, value):
       logger.debug('Broadcasting MQTT message on topic: ' + room['mqtt_availability_topic'] + ', value: ' + value)
       self.mqttclient.publish(room['mqtt_availability_topic'], payload=value, qos=1, retain=True)

    def mqtt_broadcast_state(self, room):
        topic = room['mqtt_state_topic']
        state = json.dumps(room['control'].get_state())
        logger.debug('Broadcasting MQTT message on topic: ' + topic + ', value: ' + state)
        self.mqttclient.publish(topic, payload=state, qos=2, retain=room != self.room_all)

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
