from datetime import datetime
from statistics import mean, StatisticsError
import logging

modes = ['auto', 'off', 'cool', 'heat']

class RoomControl():
    set_temp = 23
    heating_level = 0
    cooling_level = 0
    mode = 'auto'

    def __init__(self, name):
        self.name = name
        self.sensors = []
        self.adjacent_rooms = []

    def get_temp(self, fallback_to_adj=False):
        try:
            return mean(filter(None, [s.get_temp() for s in self.sensors]))
        except StatisticsError:
            if not fallback_to_adj:
                return None
            
            try:
                return mean(filter(None, [r.get_temp() for r in self.adjacent_rooms]))
            except StatisticsError:
                return None

    def update_mode(self, mode):
        if mode not in modes:
            raise ValueError('Mode {} not valid mode, must be one of: '.format(mode, ', '.join(modes)))

        self.mode = mode

    def update(self):
        temp = self.get_temp(fallback_to_adj=True)
        if temp == None:
            #logging.info('Temperature not found for room {}, HVAC is disabled'.format(self.name))
            self.heating_level = 0
            self.cooling_level = 0
            return

        if self.mode in ['auto', 'heat'] and temp < self.set_temp:
            self.heating_level = 100
        else:
            self.heating_level = 0

        if self.mode in ['auto', 'cool'] and temp > self.set_temp + 2:
            self.cooling_level = 100
        else:
            self.cooling_level = 0

    def get_state(self):
        sensors_state = [s.is_connected() for s in self.sensors]
        return {
            'temperature': self.get_temp(),
            'disconnected_sensor_count': len(sensors_state) - sum(sensors_state),
            'connected_sensor_count': sum(sensors_state),
            'mode': self.mode,
            'set_temperature': self.set_temp,
            'heating_level': self.heating_level,
            'cooling_level': self.cooling_level
        }
        
    def set_state(self, state):
        self.mode = state['mode']
        self.set_temp = state['set_temperature']
        self.heating_level = state['heating_level']
        self.cooling_level = state['cooling_level']
