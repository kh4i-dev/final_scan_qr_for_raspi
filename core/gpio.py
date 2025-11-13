# core/gpio.py
import logging
from typing import Optional

try:
    import RPi.GPIO as RPiGPIO
    REAL_GPIO = True
except (ImportError, RuntimeError):
    RPiGPIO = None
    REAL_GPIO = False

class GPIOProvider:
    def setup(self, pin: Optional[int], mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin: Optional[int], value): raise NotImplementedError
    def input(self, pin: Optional[int]): raise NotImplementedError
    def cleanup(self): raise NotImplementedError

class RealGPIO(GPIOProvider):
    def __init__(self):
        if not RPiGPIO:
            raise ImportError("RPi.GPIO not available")
        self.gpio = RPiGPIO
        for attr in ['BOARD', 'BCM', 'OUT', 'IN', 'HIGH', 'LOW', 'PUD_UP']:
            setattr(self, attr, getattr(self.gpio, attr))
        self.gpio.setmode(self.BCM)
        self.gpio.setwarnings(False)

    def setup(self, pin, mode, pull_up_down=None):
        if pin is not None:
            self.gpio.setup(pin, mode, pull_up_down=pull_up_down or self.gpio.PUD_UP)

    def output(self, pin, value):
        if pin is not None:
            self.gpio.output(pin, value)

    def input(self, pin):
        return self.gpio.input(pin) if pin is not None else self.gpio.HIGH

    def cleanup(self):
        self.gpio.cleanup()

class MockGPIO(GPIOProvider):
    def __init__(self):
        self.BOARD = "MOCK"; self.BCM = "MOCK"; self.OUT = "OUT"; self.IN = "IN"
        self.HIGH = 1; self.LOW = 0; self.PUD_UP = "UP"
        self.pin_states = {}
        self.input_pins = set()
        logging.warning("USING MOCK GPIO")

    def setup(self, pin, mode, pull_up_down=None):
        if pin is not None:
            self.pin_states[pin] = self.LOW if mode == self.OUT else self.HIGH
            if mode == self.IN:
                self.input_pins.add(pin)

    def output(self, pin, value):
        if pin is not None:
            self.pin_states[pin] = value

    def input(self, pin):
        return self.pin_states.get(pin, self.HIGH)

    def set_input(self, pin, state):
        self.pin_states[pin] = self.HIGH if state else self.LOW

    def cleanup(self): pass

def get_gpio_provider() -> GPIOProvider:
    return RealGPIO() if REAL_GPIO else MockGPIO()