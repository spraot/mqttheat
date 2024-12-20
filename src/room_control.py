from statistics import mean, StatisticsError
import logging
from pid import PID

class RoomControl():
    temperature = 23
    heating_level = 0
    cooling_level = 0
    mode = 'auto'
    control_types = ['onoff', 'pid']
    control_type = 'onoff'
    door_sensor = None

    def __init__(self, name, can_heat, can_cool):
        self.name = name
        self.can_heat = can_heat
        self.can_cool = can_cool
        self.sensors = []
        self.adjacent_rooms = []
        self.pid = PID(200, 1, 10000000, setpoint=self.temperature)
        self.pid.proportional_on_measurement = False
        self.pid.d_tau = 3600
        self._modifier_pid = 0
        self._modifier_onoff = 0

        self.modes = ['auto', 'off']
        if self.can_cool:
            self.modes.append('cool')
        if self.can_heat:
            self.modes.append('heat')

    def get_temperature(self, fallback_to_adj=False):
        try:
            return mean(filter(None, [s.getValue('temperature') for s in self.sensors]))
        except StatisticsError:
            if not fallback_to_adj:
                return None
            
            try:
                return mean(filter(None, [r.getValue('temperature') for r in self.adjacent_rooms]))
            except StatisticsError:
                return None

    def update_mode(self, mode):
        if mode not in self.modes:
            raise ValueError('Mode {} not valid mode for room {}, must be one of: '.format(mode, self.name, ', '.join(self.modes)))

        self.mode = mode

    def update(self, modifier_onoff=0, modifier_pid=0):
        self._modifier_pid = modifier_pid
        self._modifier_onoff = modifier_onoff
        temp = self.get_temperature(fallback_to_adj=True)
        if self.door_sensor and self.door_sensor.getValue('contact') == False:
            logging.info('Window/door is open in room {}, HVAC is disabled'.format(self.name))
            self.heating_level = 0
            self.cooling_level = 0

        if temp == None:
            logging.error('Temperature not found for room {}, HVAC is disabled'.format(self.name))
            self.heating_level = 0
            self.cooling_level = 0

        elif self.control_type == 'onoff':
            self._do_onoff(temp)

        elif self.control_type == 'pid':
            self._do_pid(temp)

        else:
            logging.error('control type {} not valid for room {}, HVAC is disabled', self.control_type, self.name)
            self.heating_level = 0
            self.cooling_level = 0

        if not self.can_heat or self.mode not in ['auto', 'heat']:
            self.heating_level = 0
        
        if not self.can_cool or self.mode not in ['auto', 'cool']:
            self.cooling_level = 0

    def _do_onoff(self, temp):
        logging.debug('doing on-off control')
        if temp+self._modifier_onoff < self.temperature:
            self.heating_level = 100
        else:
            self.heating_level = 0

        if temp+self._modifier_onoff > self.temperature + 0.5:
            self.cooling_level = 100
        else:
            self.cooling_level = 0

    def _do_pid(self, temp):
        logging.debug('doing PID control for room {}', self.name)
        self.pid.setpoint = self.temperature
        self.pid.integral_limits = (-200, 200)
        self.pid.output_limits = (-100, 100)

        power = self.pid(temp, modifier=self._modifier_pid)
        self.heating_level = max(0, power)
        self.cooling_level = max(0, -power)

    def get_state(self):
        sensors_state = [s.is_connected() for s in self.sensors]
        return {
            'current_temperature': self.get_temperature(),
            'disconnected_sensor_count': len(sensors_state) - sum(sensors_state),
            'connected_sensor_count': sum(sensors_state),
            'mode': self.mode,
            'temperature': self.temperature,
            'heating_level': self.heating_level,
            'cooling_level': self.cooling_level,
            'control_type': self.control_type,
            'pid_Kp': self.pid.Kp,
            'pid_Ki': self.pid.Ki,
            'pid_Kd': self.pid.Kd,
            'pid__proportional': self.pid._proportional,
            'pid__integral': self.pid._integral,
            'pid__derivative': self.pid._derivative,
            'pid__modifier': self._modifier_pid,
            'pid_d_tau': self.pid.d_tau
        }
        
    def set_state(self, state):
        try:
            val = state['temperature']
            if isinstance(val, str) and val[0] in ['+', '-']:
                self.temperature = self.temperature + float(val)
            else:
                self.temperature = float(val)
        except (KeyError, ValueError, TypeError):
            pass

        for key in ['heating_level', 'cooling_level']:
            try:
                self.__setattr__(key, float(state[key]))
            except (KeyError, ValueError, TypeError):
                pass

        for key in ['pid_Kp', 'pid_Ki', 'pid_Kd', 'pid_d_tau', 'pid__integral']:
            try:
                self.pid.__setattr__(key[4:], float(state[key]))
            except (KeyError, AttributeError, ValueError, TypeError):
                pass

        try:
            if state['mode'] in self.modes:
                self.mode = state['mode']
        except KeyError:
            pass

        try:
            if state['control_type'] in self.control_types:
                self.control_type = state['control_type']
        except KeyError:
            pass

        try:
            self._modifier_pid = float(state['pid_modifier'])
        except (KeyError, AttributeError, ValueError, TypeError):
            pass

        try:
            self._modifier_onoff =float(state['onoff_modifier'])
        except (KeyError, AttributeError, ValueError, TypeError):
            pass

        try:
            self.pid.last_output = float(state['heating_level']) if float(state['heating_level']) > 0 else -float(state['cooling_level'])
        except (KeyError, ValueError, TypeError):
            try:
                self.pid.last_output = -float(state['cooling_level']) if float(state['cooling_level']) > 0 else 0
            except (KeyError, ValueError, TypeError):
                pass
