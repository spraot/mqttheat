from datetime import datetime
from statistics import mean, StatisticsError
import logging
from pid import PID

modes = ['off', 'auto', 'cool', 'heat']

class RoomControl():
    set_temperature = 23
    heating_level = 0
    cooling_level = 0
    mode = 'auto'
    control_type = 'onoff'

    def __init__(self, name, can_heat, can_cool):
        self.name = name
        self.can_heat = can_heat
        self.can_cool = can_cool
        self.sensors = []
        self.adjacent_rooms = []
        self.pid = PID(200, 1, 10000000, setpoint=self.set_temperature)
        self.pid.proportional_on_measurement = False
        self.pid.d_tau = 3600

    def get_temperature(self, fallback_to_adj=False):
        try:
            return mean(filter(None, [s.get_temperature() for s in self.sensors]))
        except StatisticsError:
            if not fallback_to_adj:
                return None
            
            try:
                return mean(filter(None, [r.get_temperature() for r in self.adjacent_rooms]))
            except StatisticsError:
                return None

    def update_mode(self, mode):
        if mode not in modes:
            raise ValueError('Mode {} not valid mode, must be one of: '.format(mode, ', '.join(modes)))

        self.mode = mode

    def update(self):
        temp = self.get_temperature(fallback_to_adj=True)
        if temp == None:
            logging.debug('Temperature not found for room {}, HVAC is disabled'.format(self.name))
            self.heating_level = 0
            self.cooling_level = 0

        elif self.control_type == 'onoff':
            self.do_onoff(temp)

        elif self.control_type == 'pid':
            self.do_pid(temp)

        else:
            logging.error('control type {} not valid for room {}, HVAC is disabled'.format(self.control_type, self.name))
            self.heating_level = 0
            self.cooling_level = 0

    def do_onoff(self, temp):
        if self.can_heat and self.mode in ['auto', 'heat'] and temp < self.set_temperature:
            self.heating_level = 100
        else:
            self.heating_level = 0

        if self.can_cool and self.mode in ['auto', 'cool'] and temp > self.set_temperature + 1:
            self.cooling_level = 100
        else:
            self.cooling_level = 0

    def do_pid(self, temp):
        self.pid.setpoint = self.set_temperature
        self.pid.output_limits = (-100,100)
#            -100 if self.can_cool and self.mode in ['auto', 'cool'] else 0,
#            100 if self.can_heat and self.mode in ['auto', 'heat'] else 0
#        )

        power = self.pid(temp)
        self.heating_level = max(0, power)
        self.cooling_level = max(0, -power)

    def get_state(self):
        sensors_state = [s.is_connected() for s in self.sensors]
        return {
            'temperature': self.get_temperature(),
            'disconnected_sensor_count': len(sensors_state) - sum(sensors_state),
            'connected_sensor_count': sum(sensors_state),
            'mode': self.mode,
            'set_temperature': self.set_temperature,
            'heating_level': self.heating_level,
            'cooling_level': self.cooling_level,
            'control_type': self.control_type,
            'pid_Kp': self.pid.Kp,
            'pid_Ki': self.pid.Ki,
            'pid_Kd': self.pid.Kd,
            'pid_p': self.pid._proportional,
            'pid_i': self.pid._integral,
            'pid_d': self.pid._derivative,
            'pid_d_tau': self.pid.d_tau
        }
        
    def set_state(self, state):
        for key in ['mode', 'set_temperature', 'heating_level', 'cooling_level', 'control_type']:
            try:
                self.__setattr__(key, state[key])
            except KeyError:
                pass

        for key in ['pid_Kp', 'pid_Ki', 'pid_Kd', 'pid_d_tau']:
            try:
                self.pid.__setattr__(key[4:], state[key])
            except KeyError:
                pass

        try:
            self.pid.last_output = state['heating_level'] if state['heating_level'] > 0 else -state['cooling_level']
        except KeyError:
            try:
                self.pid.last_output = -state['cooling_level'] if state['cooling_level'] > 0 else 0
            except KeyError:
                pass
