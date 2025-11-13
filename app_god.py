import cv2
import time
import json
import os
import logging
from logging.handlers import RotatingFileHandler

import threading
import sqlite3 
import copy     
import uuid     
from datetime import datetime 
import requests
import io      
try:
    from ultralytics import YOLO
    YOLO_ENABLED = True
except ImportError:
    YOLO_ENABLED = False
    logging.warning("[AI] Thư viện 'ultralytics' chưa được cài đặt (pip install ultralytics). Tính năng AI sẽ bị tắt.")
import os
import functools
try:
    from pyzbar import pyzbar
    PYZBAR_ENABLED = True
except ImportError:
    PYZBAR_ENABLED = False
    logging.warning("[QR] Thư viện pyzbar chưa được cài đặt (pip install pyzbar). Sẽ chỉ dùng cv2.QRCodeDetector().")
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, Response, jsonify, request
from flask_sock import Sock
import unicodedata, re # (ĐÃ GIỮ NGUYÊN BẢN ĐÚNG)

# [NÂNG CẤP DEEPSORT] Thêm import cho DeepSORT
try:
    from deep_sort_realtime.deepsort_tracker import DeepSort
    DEEPSORT_ENABLED = True
except ImportError:
    DEEPSORT_ENABLED = False
    logging.warning("[DEEPSORT] Thư viện deep-sort-realtime chưa được cài đặt (pip install deep-sort-realtime). Tracking bị tắt.")

# Thêm cho ví dụ kiểm tra
import unittest
import numpy as np  # Để tạo frame giả cho test
import argparse  # Để chạy test từ command line

# =============================
#       CẤU HÌNH KẾT NỐI VPS
# =============================
VPS_URL = "https://phanloai.kh4idev.id.vn" # (ĐÃ GIỮ NGUYÊN BẢN ĐÚNG)
PI_API_KEY = os.environ.get("PI_API_KEY", "your-very-secret-key-12345")

# =============================
#       CÁC HÀM TIỆN ÍCH CHUẨN HOÁ
# =============================
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def canon_id(s: str) -> str:
    if s is None: return ""
    s = str(s).strip()
    try: s = s.encode("utf-8").decode("unicode_escape")
    except Exception: pass
    s = _strip_accents(s).upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    s = re.sub(r"^(LOAI|LO)+", "", s)
    return s

# =============================
#       LỚP TRỪU TƯỢNG GPIO
# =============================
try:
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    RPiGPIO = None

class GPIOProvider:
    def setup(self, pin, mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin, value): raise NotImplementedError
    def input(self, pin): raise NotImplementedError
    def cleanup(self): raise NotImplementedError
    def setmode(self, mode): raise NotImplementedError
    def setwarnings(self, value): raise NotImplementedError

class RealGPIO(GPIOProvider):
    def __init__(self):
        if RPiGPIO is None: raise ImportError("Không thể tải RPi.GPIO.")
        self.gpio = RPiGPIO
        for attr in ['BOARD', 'BCM', 'OUT', 'IN', 'HIGH', 'LOW', 'PUD_UP']:
            setattr(self, attr, getattr(self.gpio, attr))
    def setmode(self, mode): self.gpio.setmode(mode)
    def setwarnings(self, value): self.gpio.setwarnings(value)
    def setup(self, pin, mode, pull_up_down=None):
        if pin is not None:
            if pull_up_down: self.gpio.setup(pin, mode, pull_up_down=pull_up_down)
            else: self.gpio.setup(pin, mode)
    def output(self, pin, value): 
        if pin is not None: self.gpio.output(pin, value)
    def input(self, pin): 
        if pin is not None: return self.gpio.input(pin)
        return self.gpio.HIGH
    def cleanup(self): self.gpio.cleanup()

class MockGPIO(GPIOProvider):
    def __init__(self):
        self.BOARD = "mock_BOARD"; self.BCM = "mock_BCM"; self.OUT = "mock_OUT"
        self.IN = "mock_IN"; self.HIGH = 1; self.LOW = 0
        self.input_pins = set(); self.PUD_UP = "mock_PUD_UP"; self.pin_states = {}
        logging.warning("="*50 + "\nĐANG CHẠY Ở CHẾ ĐỘ GIẢ LẬP (MOCK GPIO).\n" + "="*50)
    def setmode(self, mode): logging.info(f"[MOCK] setmode={mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] setwarnings={value}")
    def setup(self, pin, mode, pull_up_down=None):
        if pin is not None:
            logging.info(f"[MOCK] setup pin {pin} mode={mode} pull_up_down={pull_up_down}")
            if mode == self.OUT: self.pin_states[pin] = self.LOW
            else: self.pin_states[pin] = self.HIGH; self.input_pins.add(pin)
    def output(self, pin, value):
        if pin is not None:
            logging.info(f"[MOCK] output pin {pin}={value}")
            self.pin_states[pin] = value
    def input(self, pin):
        if pin is not None: return self.pin_states.get(pin, self.HIGH)
        return self.HIGH
    def set_input_state(self, pin, logical_state):
        if pin not in self.input_pins: self.input_pins.add(pin)
        state = self.HIGH if logical_state else self.LOW
        self.pin_states[pin] = state
        logging.info(f"[MOCK] set_input_state pin {pin} -> {state}")
        return state
    def toggle_input_state(self, pin):
        if pin not in self.input_pins: self.input_pins.add(pin)
        new_state = self.LOW if self.input() == self.HIGH else self.HIGH
        self.pin_states[pin] = new_state
        logging.info(f"[MOCK] toggle_input_state pin {pin} -> {new_state}")
        return 0 if new_state == self.LOW else 1
    def cleanup(self): logging.info("[MOCK] cleanup GPIO")

def get_gpio_provider():
    if RPiGPIO: return RealGPIO()
    return MockGPIO()

# =============================
#   QUẢN LÝ LỖI (Error Manager)
# =============================
class ErrorManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.maintenance_mode = False
        self.last_error = None
    def trigger_maintenance(self, message):
        with self.lock:
            if self.maintenance_mode: return
            self.maintenance_mode = True
            self.last_error = message
            logging.critical("="*50 + f"\n[MAINTENANCE MODE] Lỗi nghiêm trọng: {message}\n" + "="*50)
            broadcast_log({"log_type": "error", "message": f"MAINTENANCE MODE: {message}"})
    def reset(self):
        with self.lock:
            self.maintenance_mode = False
            self.last_error = None
            logging.info("[MAINTENANCE MODE] Đã reset chế độ bảo trì.")
            with state_lock:
                for lane in system_state["lanes"]:
                    lane["status"] = "Sẵn sàng"
    def is_maintenance(self):
        return self.maintenance_mode

# =============================
#       CẤU HÌNH CHUNG
# =============================

# =============================
# CẤU HÌNH THƯ MỤC CƠ BẢN
# =============================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))             # Thư mục gốc chứa app.py
CONFIG_DIR = os.path.join(BASE_DIR, "config")                     # Thư mục chứa config JSON
LOG_DIR = os.path.join(BASE_DIR, "logs")                          # Thư mục chứa log, DB, v.v.
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# =============================
#  ĐƯỜNG DẪN FILE CẤU HÌNH & LOG
# =============================

CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
LOG_FILE = os.path.join(LOG_DIR, "system.log")
DATABASE_FILE = os.path.join(LOG_DIR, "sort_log.db")
QUEUE_STATE_FILE = os.path.join(LOG_DIR, "queue_state.json")

# =============================
#  THIẾT LẬP GHI LOG XOAY VÒNG (ROTATING LOG)
# =============================

handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=2_000_000,
    backupCount=5,
    encoding="utf-8"
)

logging.basicConfig(
    handlers=[handler],
    level=logging.INFO,  # Mức log: DEBUG / INFO / WARNING / ERROR / CRITICAL
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# In ra terminal + file luôn
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(console)

logging.info("[SYSTEM] Log system initialized ✅")
logging.info(f"[PATH] CONFIG_FILE: {CONFIG_FILE}")
logging.info(f"[PATH] LOG_FILE: {LOG_FILE}")
logging.info(f"[PATH] DATABASE_FILE: {DATABASE_FILE}")
logging.info(f"[PATH] QUEUE_STATE_FILE: {QUEUE_STATE_FILE}")

CAMERA_INDEX = 1
ACTIVE_LOW = True
AUTH_ENABLED = os.environ.get("APP_AUTH_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
USERNAME = os.environ.get("APP_USERNAME", "admin")
PASSWORD = os.environ.get("APP_PASSWORD", "123")
SENSOR_ENTRY_PIN = 6 # (Từ v2)
SENSOR_ENTRY_MOCK_PIN = 99 # (Từ v2)

# =============================
#     KHỞI TẠO CÁC ĐỐI TƯỢNG
# =============================
GPIO = get_gpio_provider()
error_manager = ErrorManager()
executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="TestWorker")
database_lock = threading.Lock()
config_file_lock = threading.Lock()

# =============================
#       KHAI BÁO CHÂN GPIO
# =============================
DEFAULT_LANES_CONFIG = [
    {"id": "SP001", "name": "Phân loại A", "sensor_pin": 5, "push_pin": 12, "pull_pin": 11},
    {"id": "SP002", "name": "Phân loại B", "sensor_pin": 16, "push_pin": 13, "pull_pin": 8},
    {"id": "SP003", "name": "Phân loại C", "sensor_pin": 18, "push_pin": 15, "pull_pin": 7},
    {"id": "NG", "name": "Sản Phẩm NG(Bỏ)", "sensor_pin": None, "push_pin": None, "pull_pin": None},
]
lanes_config = DEFAULT_LANES_CONFIG
RELAY_PINS = []
SENSOR_PINS = []
RELAY_CONVEYOR_PIN = 7

# =============================
#     HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    if pin is not None:
        try: GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
        except Exception as e:
            logging.error(f"[GPIO] Lỗi RELAY_ON pin {pin}: {e}")
            error_manager.trigger_maintenance(f"Lỗi GPIO pin {pin}: {e}")
def RELAY_OFF(pin):
    if pin is not None:
        try: GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)
        except Exception as e:
            logging.error(f"[GPIO] Lỗi RELAY_OFF pin {pin}: {e}")
            error_manager.trigger_maintenance(f"Lỗi GPIO pin {pin}: {e}")

def CONVEYOR_RUN():
    logging.info("[CONVEYOR] Băng chuyền: RUN")
    RELAY_ON(RELAY_CONVEYOR_PIN)

def CONVEYOR_STOP():
    logging.info("[CONVEYOR] Băng chuyền: STOP")
    RELAY_OFF(RELAY_CONVEYOR_PIN)

# =============================
#       TRẠNG THÁI HỆ THỐNG
# =============================
system_state = {
    "lanes": [],
    "timing_config": {
        "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
        "push_delay": 0.0, "gpio_mode": "BOARD",
        "queue_head_timeout": 15.0,
        "pending_trigger_timeout": 0.5, # Không còn dùng
        "RELAY_CONVEYOR_PIN": None,
        "stop_conveyor_on_entry": False, # (Dùng cho v2)
        "stability_delay": 0.25, # (Dùng cho v2)
        "stop_conveyor_on_qr": False, # (Dùng cho v1)
        "conveyor_stop_delay_qr": 2.0, # (Dùng cho v1)
        "qr_debounce_time": 3.0, # (CẬP NHẬT) Thêm debounce cho logic v1
        "use_sensor_entry_gantry": False # --- (MERGE) Cờ Bật/Tắt
    },
    "is_mock": isinstance(GPIO, MockGPIO), "maintenance_mode": False,
    "auth_enabled": AUTH_ENABLED, "gpio_mode": "BOARD", "last_error": None,
    "queue_indices": [],
    "sensor_entry_reading": 1, # (Từ v2)
    "entry_queue_size": 0,
    "ai_config": {},
    "camera_settings": {}
}

state_lock = threading.Lock()
main_loop_running = True
latest_frame = None
frame_lock = threading.Lock()
fps_value = 0.0

AI_MODEL = None
AI_ENABLED = False
AI_LANE_MAP = {}
AI_MIN_CONFIDENCE = 0.6

# [NÂNG CẤP DEEPSORT] Thêm tracker toàn cục
DEEPSORT_TRACKER = None

qr_queue = [] # (Dùng cho v2)
qr_queue_lock = threading.Lock()

processing_queue = [] # Hàng chờ chính (cả v1 và v2 đều dùng)
processing_queue_lock = threading.Lock()

QUEUE_HEAD_TIMEOUT = 15.0
queue_head_since = 0.0

last_sensor_state = []
last_sensor_trigger_time = []
AUTO_TEST_ENABLED = False 
auto_test_last_state = [] 
auto_test_last_trigger = []

last_entry_sensor_state = 1 # (Từ v2)
last_entry_sensor_trigger_time = 0.0 # (Từ v2)

# =============================
# KHỞI TẠO CƠ SỞ DỮ LIỆU
# =============================
def init_database():
    with database_lock:
        try:
            conn = sqlite3.connect(DATABASE_FILE)
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sort_log (
                date TEXT,
                lane_name TEXT,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (date, lane_name)
            )
            """)
            conn.commit()
            conn.close()
            logging.info(f"[DB] Đã khởi tạo CSDL SQLite tại '{DATABASE_FILE}' thành công.")
        except Exception as e:
            logging.critical(f"[CRITICAL] Không thể khởi tạo CSDL SQLite: {e}")
            error_manager.trigger_maintenance(f"Lỗi khởi tạo CSDL SQLite: {e}")

# =============================
#     LƯU/TẢI HÀNG CHỜ
# =============================
def save_queues_on_shutdown():
    logging.info("[SHUTDOWN] Đang lưu trạng thái hàng chờ...")
    try:
        queue_data = {}
        with qr_queue_lock:
            queue_data['qr_queue'] = list(qr_queue) # (Lưu hàng chờ v2)
        with processing_queue_lock:
            queue_data['processing_queue'] = list(processing_queue) # (Lưu hàng chờ v1/v2)
        
        if not queue_data['qr_queue'] and not queue_data['processing_queue']:
            logging.info("[SHUTDOWN] Hàng chờ trống, không cần lưu.")
            if os.path.exists(QUEUE_STATE_FILE):
                os.remove(QUEUE_STATE_FILE)
            return

        with open(QUEUE_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(queue_data, f)
        logging.info(f"[SHUTDOWN] Đã lưu {len(queue_data['qr_queue'])} QR, {len(queue_data['processing_queue'])} Job vào {QUEUE_STATE_FILE}")
    except Exception as e:
        logging.error(f"[SHUTDOWN] Lỗi không thể lưu hàng chờ: {e}")

def load_queues_on_startup():
    global qr_queue, processing_queue, queue_head_since
    if os.path.exists(QUEUE_STATE_FILE):
        logging.warning(f"[STARTUP] Phát hiện file {QUEUE_STATE_FILE}. Đang khôi phục hàng chờ...")
        try:
            with open(QUEUE_STATE_FILE, 'r', encoding='utf-8') as f:
                queue_data = json.load(f)
            
            with qr_queue_lock:
                qr_queue = queue_data.get('qr_queue', [])
            with processing_queue_lock:
                processing_queue = queue_data.get('processing_queue', [])
                if processing_queue:
                    queue_head_since = time.time() # Reset timeout
            
            logging.info(f"[STARTUP] Đã khôi phục {len(qr_queue)} QR, {len(processing_queue)} Job.")
            
            # Cập nhật state (lấy từ v1)
            with state_lock:
                    system_state["queue_indices"] = [j["lane_index"] for j in processing_queue]
                    system_state["entry_queue_size"] = len(processing_queue)
                    for job in processing_queue:
                        lane_idx = job.get('lane_index')
                        if 0 <= lane_idx < len(system_state['lanes']):
                             system_state['lanes'][lane_idx]['status'] = "Đang chờ vật (Tải lại)"

        except Exception as e:
            logging.error(f"[STARTUP] Lỗi khôi phục hàng chờ: {e}. Bắt đầu với hàng chờ trống.")
            qr_queue = []; processing_queue = []
        
        try:
            os.remove(QUEUE_STATE_FILE)
            logging.info(f"[STARTUP] Đã xử lý và xóa file {QUEUE_STATE_FILE}.")
        except Exception as e:
            logging.error(f"[STARTUP] Lỗi xóa file {QUEUE_STATE_FILE}: {e}")
    else:
        logging.info("[STARTUP] Không có file trạng thái hàng chờ. Bắt đầu mới.")

# =============================
#     HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    global lanes_config, RELAY_PINS, SENSOR_PINS, last_sensor_state, last_sensor_trigger_time
    global auto_test_last_state, auto_test_last_trigger
    global QUEUE_HEAD_TIMEOUT, RELAY_CONVEYOR_PIN
    global AI_MODEL, AI_ENABLED, AI_LANE_MAP, AI_MIN_CONFIDENCE
    global DEEPSORT_TRACKER  # [NÂNG CẤP DEEPSORT] Thêm biến tracker

    # [NÂNG CẤP YOLOv8] Cập nhật default_ai_config với tùy chọn nâng cao cho YOLOv8
    default_ai_config = {
        "enable_ai": False,
        "ai_priority": False,
        "model_path": "yolov8n.pt",  # [NÂNG CẤP YOLOv8] Mặc định dùng YOLOv8 nano cho tốc độ
        "min_confidence": 0.6,
        "yolo_iou": 0.45,  # [NÂNG CẤP YOLOv8] Ngưỡng IoU cho NMS
        "yolo_augment": False,  # [NÂNG CẤP YOLOv8] Bật augmentation cho độ chính xác cao hơn (giảm tốc độ)
        "yolo_half": False,  # [NÂNG CẤP YOLOv8] Bật FP16 cho tốc độ trên GPU
        "enable_deepsort": True,  # [NÂNG CẤP DEEPSORT] Tùy chọn bật DeepSORT
        "deepsort_max_age": 30,   # [NÂNG CẤP DEEPSORT] Tham số mặc định
        "deepsort_n_init": 3,
        "deepsort_max_iou_distance": 0.7,  # [NÂNG CẤP YOLOv8] Thêm param nâng cao cho DeepSORT
        "ai_class_to_id_map": {
            "APPLE": "SP001",
            "ORANGE": "SP002",
            "BANANA": "SP003"
        }
    }
    default_timing_config = {
        "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
        "push_delay": 0.0, "gpio_mode": "BOARD",
        "queue_head_timeout": 15.0, "pending_trigger_timeout": 0.5,
        "RELAY_CONVEYOR_PIN": None,
        "stop_conveyor_on_entry": False,
        "stability_delay": 0.25,
        "stop_conveyor_on_qr": False,
        "conveyor_stop_delay_qr": 2.0,
        "qr_debounce_time": 3.0, # (CẬP NHẬT) Thêm debounce cho logic v1
        "use_sensor_entry_gantry": False # --- (MERGE) Thêm cờ Gantry, mặc định là TẮT (chạy logic v1)
    }
    default_camera_settings = {
        "auto_exposure": False,
        "brightness": 128,
        "contrast": 32
    }
    default_config_full = {
        "timing_config": default_timing_config,
        "lanes_config": DEFAULT_LANES_CONFIG,
        "ai_config": default_ai_config,
        "camera_settings": default_camera_settings
    }    
    loaded_config = default_config_full
    
    with config_file_lock:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f: file_content = f.read()
                if not file_content:
                    logging.warning("[CONFIG] File config rỗng, dùng mặc định.")
                else:
                    loaded_config_from_file = json.loads(file_content)
                    
                    timing_cfg = default_timing_config.copy()
                    timing_cfg.update(loaded_config_from_file.get('timing_config', {}))
                    loaded_config['timing_config'] = timing_cfg
                    
                    ai_cfg = default_ai_config.copy()
                    ai_cfg.update(loaded_config_from_file.get('ai_config', {}))
                    loaded_config['ai_config'] = ai_cfg

                    cam_cfg = default_camera_settings.copy()
                    cam_cfg.update(loaded_config_from_file.get('camera_settings', {}))
                    loaded_config['camera_settings'] = cam_cfg

                    lanes_from_file = loaded_config_from_file.get('lanes_config', DEFAULT_LANES_CONFIG)
                    loaded_config['lanes_config'] = ensure_lane_ids(lanes_from_file)
            
            except Exception as e:
                logging.error(f"[CONFIG] Lỗi đọc/parse file config ({e}), dùng mặc định.")
                error_manager.trigger_maintenance(f"Lỗi JSON file config.json: {e}")
                loaded_config = default_config_full
        else:
            logging.warning("[CONFIG] Không có file config, dùng mặc định và tạo mới.")
            try:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(loaded_config, f, indent=4)
            except Exception as e:
                logging.error(f"[CONFIG] Không thể tạo file config mới: {e}")

    lanes_config = loaded_config['lanes_config']
    num_lanes = len(lanes_config)
    new_system_lanes = []
    RELAY_PINS = []; SENSOR_PINS = []
    
    # (MERGE) Thêm pin gantry (từ v2)
    if SENSOR_ENTRY_PIN: SENSOR_PINS.append(SENSOR_ENTRY_PIN)
    if isinstance(GPIO, MockGPIO) and SENSOR_ENTRY_MOCK_PIN:
        SENSOR_PINS.append(SENSOR_ENTRY_MOCK_PIN)
        
    RELAY_CONVEYOR_PIN = loaded_config['timing_config'].get('RELAY_CONVEYOR_PIN')
    if RELAY_CONVEYOR_PIN:
        RELAY_PINS.append(RELAY_CONVEYOR_PIN)
        logging.info(f"[CONFIG] Đã cấu hình Relay Băng chuyền tại pin: {RELAY_CONVEYOR_PIN}")

    for i, lane_cfg in enumerate(lanes_config):
        lane_name = lane_cfg.get("name", f"Lane {i+1}"); lane_id = lane_cfg.get("id", f"LANE_{i+1}")
        new_system_lanes.append({
            "name": lane_name, "id": lane_id, "status": "Sẵn sàng", "count": 0,
            "sensor_pin": lane_cfg.get("sensor_pin"), "push_pin": lane_cfg.get("push_pin"),
            "pull_pin": lane_cfg.get("pull_pin"), "sensor_reading": 1,
            "relay_grab": 0, "relay_push": 0
        })
        if lane_cfg.get("sensor_pin") is not None: SENSOR_PINS.append(lane_cfg["sensor_pin"])
        if lane_cfg.get("push_pin") is not None: RELAY_PINS.append(lane_cfg["push_pin"])
        if lane_cfg.get("pull_pin") is not None: RELAY_PINS.append(lane_cfg["pull_pin"])

    # (MERGE) Thêm state cho Auto-Test (từ v1)
    last_sensor_state = [1] * num_lanes; last_sensor_trigger_time = [0.0] * num_lanes
    auto_test_last_state = [1] * num_lanes; auto_test_last_trigger = [0.0] * num_lanes

    with state_lock:
        system_state['timing_config'] = loaded_config['timing_config']
        system_state['gpio_mode'] = loaded_config['timing_config'].get("gpio_mode", "BOARD")
        system_state['lanes'] = new_system_lanes
        system_state['auth_enabled'] = AUTH_ENABLED
        system_state['is_mock'] = isinstance(GPIO, MockGPIO)
        system_state['sensor_entry_reading'] = 1 # (Từ v2)
        system_state['entry_queue_size'] = 0
        system_state['ai_config'] = loaded_config['ai_config']
        system_state['camera_settings'] = loaded_config['camera_settings']
    
    QUEUE_HEAD_TIMEOUT = loaded_config['timing_config'].get('queue_head_timeout', 15.0)

    AI_MIN_CONFIDENCE = loaded_config['ai_config'].get('min_confidence', 0.6)
    
    if loaded_config['ai_config'].get('enable_ai', False):
        if not YOLO_ENABLED:
            logging.error("[AI] Config bật AI, nhưng thư viện 'ultralytics' chưa được cài đặt. AI đã bị tắt.")
        else:
            model_path = loaded_config['ai_config'].get('model_path', 'yolov8n.pt')  # [NÂNG CẤP YOLOv8] Mặc định YOLOv8 nano
            if not os.path.exists(model_path):
                logging.error(f"[AI] Lỗi: Không tìm thấy file model tại '{model_path}'. AI đã bị tắt.")
            else:
                try:
                    AI_MODEL = YOLO(model_path)
                    AI_ENABLED = True
                    logging.info(f"[AI] Đã tải thành công model YOLOv8 từ '{model_path}'.")
                    
                    lane_id_to_index_map = {canon_id(lane['id']): i for i, lane in enumerate(lanes_config) if lane.get('id')}
                    
                    ai_class_map_config = loaded_config['ai_config'].get('ai_class_to_id_map', {})
                    for class_name, lane_id in ai_class_map_config.items():
                        canon_lane_id = canon_id(lane_id)
                        if canon_lane_id in lane_id_to_index_map:
                            lane_index = lane_id_to_index_map[canon_lane_id]
                            AI_LANE_MAP[class_name.upper()] = lane_index
                            logging.info(f"[AI] Đã map Class '{class_name.upper()}' -> Lane ID '{lane_id}' (index {lane_index})")
                        else:
                            logging.warning(f"[AI] Lỗi map: Lane ID '{lane_id}' (cho class '{class_name}') không tồn tại trong lanes_config.")
                    
                except Exception as e:
                    logging.error(f"[AI] Lỗi nghiêm trọng khi tải model YOLOv8: {e}. AI đã bị tắt.", exc_info=True)
                    AI_MODEL = None
                    AI_ENABLED = False
    
    # [NÂNG CẤP DEEPSORT & YOLOv8] Khởi tạo DeepSORT nếu bật, với param nâng cao
    if AI_ENABLED and loaded_config['ai_config'].get('enable_deepsort', False) and DEEPSORT_ENABLED:
        try:
            DEEPSORT_TRACKER = DeepSort(
                max_age=loaded_config['ai_config'].get('deepsort_max_age', 30),
                n_init=loaded_config['ai_config'].get('deepsort_n_init', 3),
                max_iou_distance=loaded_config['ai_config'].get('deepsort_max_iou_distance', 0.7),  # [NÂNG CẤP YOLOv8] Thêm param nâng cao
                nms_max_overlap=1.0,
                max_cosine_distance=0.3,
                nn_budget=None,
                embedder="mobilenet_v2"  # Mặc định, có thể thay đổi trong config
            )
            logging.info("[DEEPSORT] Đã khởi tạo tracker thành công với param nâng cao.")
        except Exception as e:
            logging.error(f"[DEEPSORT] Lỗi khởi tạo tracker: {e}. Tắt tracking.")
            DEEPSORT_TRACKER = None
    elif not DEEPSORT_ENABLED:
        logging.warning("[DEEPSORT] Không thể bật tracking do thiếu thư viện.")
    
    if not AI_ENABLED:
        logging.warning("[AI] Tính năng AI hiện đang TẮT (do config hoặc lỗi).")
    logging.info(f"[CONFIG] Loaded {num_lanes} lanes config.")
    logging.info(f"[CONFIG] Queue Timeout: {QUEUE_HEAD_TIMEOUT}s")
    logging.info(f"[CONFIG] Sensor Entry Pin (Real/Mock): {SENSOR_ENTRY_PIN} / {SENSOR_ENTRY_MOCK_PIN}")

def ensure_lane_ids(lanes_list):
    default_ids = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K']
    for i, lane in enumerate(lanes_list):
        if 'id' not in lane or not lane['id']:
            if i < len(default_ids): lane['id'] = default_ids[i]
            else: lane['id'] = f"LANE_{i+1}"
            logging.warning(f"[CONFIG] Lane {i+1} thiếu ID. Đã gán ID: {lane['id']}")
    return lanes_list

def reset_all_relays_to_default():
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT, BĂNG CHUYỀN CHẠY).")
    with state_lock:
        for lane in system_state["lanes"]:
            pull_pin = lane.get("pull_pin")
            push_pin = lane.get("push_pin")
            if pull_pin is not None: RELAY_ON(pull_pin)
            if push_pin is not None: RELAY_OFF(push_pin)
            lane["relay_grab"] = 1 if pull_pin is not None else 0
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"
    CONVEYOR_RUN() 
    time.sleep(0.1)
    logging.info("[GPIO] Reset hoàn tất.")

def periodic_config_save():
    while main_loop_running:
        time.sleep(60)
        if error_manager.is_maintenance(): continue
        
        config_to_save = {}
        
        try:
            with state_lock:
                config_to_save['timing_config'] = system_state['timing_config'].copy()
                config_to_save['ai_config'] = system_state['ai_config'].copy()
                config_to_save['camera_settings'] = system_state['camera_settings'].copy()
                
                current_lanes_config = []
                for lane_state in system_state['lanes']:
                    current_lanes_config.append({
                        "id": lane_state['id'], "name": lane_state['name'],
                        "sensor_pin": lane_state.get('sensor_pin'), 
                        "push_pin": lane_state.get('push_pin'), 
                        "pull_pin": lane_state.get('pull_pin')
                    })
                config_to_save['lanes_config'] = current_lanes_config
            
            with config_file_lock:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(config_to_save, f, indent=4)
            logging.info("[CONFIG] Đã tự động lưu config (timing, ai, lanes, camera).")

        except Exception as e:
            logging.error(f"[CONFIG] Lỗi tự động lưu config: {e}") 

# =============================
#       LUỒNG CAMERA
# =============================
def camera_capture_thread():
    global latest_frame, fps_value

    frame_count = 0
    start_time = time.time()
    
    cam_settings = {}
    with state_lock:
        cam_settings = system_state.get('camera_settings', {})
    
    camera = cv2.VideoCapture(CAMERA_INDEX)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640); 
    camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    camera.set(cv2.CAP_PROP_BUFFERSIZE, 1) 
    
    try:
        auto_exposure_cfg = cam_settings.get('auto_exposure', True)
        auto_exposure_val = 1 if auto_exposure_cfg else 0
        camera.set(cv2.CAP_PROP_AUTO_EXPOSURE, auto_exposure_val)
        logging.info(f"[CAMERA] Đã đặt Auto Exposure: {'BẬT' if auto_exposure_val == 1 else 'TẮT'}.")

        if auto_exposure_val == 0:
            brightness_val = int(cam_settings.get('brightness', 128))
            contrast_val = int(cam_settings.get('contrast', 32))
            
            camera.set(cv2.CAP_PROP_BRIGHTNESS, brightness_val)
            camera.set(cv2.CAP_PROP_CONTRAST, contrast_val)
            
            logging.info(f"[CAMERA] Đã đặt Brightness thủ công: {brightness_val}")
            logging.info(f"[CAMERA] Đã đặt Contrast thủ công: {contrast_val}")
    except Exception as cam_e:
        logging.error(f"[CAMERA] Lỗi khi cài đặt thông số camera: {cam_e}")
    
    if not camera.isOpened():
        logging.error("[ERROR] Không mở được camera.")
        error_manager.trigger_maintenance("Không thể mở camera.")
        return
    
    logging.info("[CAMERA] Camera đã khởi động.")
    
    retries = 0; 
    max_retries = 5
    while main_loop_running:
        if error_manager.is_maintenance():
            time.sleep(0.5); continue
            
        ret, frame = camera.read()
        
        if not ret:
            retries += 1
            logging.warning(f"[WARN] Mất camera (lần {retries}/{max_retries}), thử khởi động lại...")
            broadcast_log({"log_type":"error","message":f"Mất camera (lần {retries}), đang thử lại..."})
            if retries > max_retries:
                logging.critical("[ERROR] Camera lỗi vĩnh viễn. Chuyển sang chế độ bảo trì.")
                error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                break
            camera.release(); time.sleep(1); camera = cv2.VideoCapture(CAMERA_INDEX)
            continue
        retries = 0

        frame_count += 1
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        if elapsed_time >= 1.0:
            fps_value = frame_count / elapsed_time
            frame_count = 0
            start_time = current_time
        
        with frame_lock:
            latest_frame = frame.copy()
            
        time.sleep(1 / 60) # Cung cấp 60 FPS
        
    camera.release()

# =============================
#     LƯU LOG ĐẾM SẢN PHẨM
# =============================
def log_sort_count(lane_index, lane_name):
    with database_lock:
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            conn = sqlite3.connect(DATABASE_FILE)
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO sort_log (date, lane_name, count)
            VALUES (?, ?, 1)
            ON CONFLICT(date, lane_name) DO UPDATE SET
                count = count + 1
            """, (today, lane_name))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"[DB] Lỗi khi ghi log đếm vào SQLite: {e}")

# =============================
#     CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index, job_id="N/A"): 
    job_id_log_prefix = f"[JobID {job_id}]"
    
    lane_name = ""; push_pin, pull_pin = None, None
    is_sorting_lane = False
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                logging.error(f"[SORT] {job_id_log_prefix} Lane index {lane_index} không hợp lệ.")
                return
            cfg = system_state['timing_config']
            delay = cfg['cycle_delay']; settle_delay = cfg['settle_delay']
            lane = system_state["lanes"][lane_index]
            lane_name = lane["name"]; push_pin = lane.get("push_pin"); pull_pin = lane.get("pull_pin")
            is_sorting_lane = not (push_pin is None and pull_pin is None)
            if is_sorting_lane and (push_pin is None or pull_pin is None):
                logging.error(f"[SORT] {job_id_log_prefix} Lane {lane_name} (index {lane_index}) chưa được cấu hình đủ chân relay.")
                lane["status"] = "Lỗi Config"
                broadcast_log({"log_type": "error", "message": f"{job_id_log_prefix} Lane {lane_name} thiếu cấu hình chân relay."})
                return
            lane["status"] = "Đang phân loại..." if is_sorting_lane else "Đang đi thẳng..."
        
        if not is_sorting_lane:
            broadcast_log({"log_type": "info", "message": f"{job_id_log_prefix} Vật phẩm đi thẳng qua {lane_name}"})
        if is_sorting_lane:
            broadcast_log({"log_type": "info", "message": f"{job_id_log_prefix} Bắt đầu chu trình đẩy {lane_name}"})
            RELAY_OFF(pull_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
            time.sleep(settle_delay);
            if not main_loop_running: return
            RELAY_ON(push_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
            time.sleep(delay);
            if not main_loop_running: return
            RELAY_OFF(push_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
            time.sleep(settle_delay);
            if not main_loop_running: return
            RELAY_ON(pull_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1

    except Exception as e:
        logging.error(f"[SORT] {job_id_log_prefix} Lỗi trong sorting_process (lane {lane_name}): {e}")
        error_manager.trigger_maintenance(f"Lỗi sorting_process (Lane {lane_name}): {e}")
    finally:
        with state_lock:
            if 0 <= lane_index < len(system_state["lanes"]):
                lane = system_state["lanes"][lane_index]
                if lane_name and lane["status"] != "Lỗi Config":
                    lane["count"] += 1
                    log_type = "sort" if is_sorting_lane else "pass"
                    broadcast_log({"log_type": log_type, "name": lane_name, "count": lane['count']})
                    log_sort_count(lane_index, lane_name)
                    if lane["status"] != "Lỗi Config":
                        lane["status"] = "Sẵn sàng"
        if lane_name:
            msg = f"Hoàn tất chu trình cho {lane_name}" if is_sorting_lane else f"Hoàn tất đếm vật phẩm đi thẳng qua {lane_name}"
            broadcast_log({"log_type": "info", "message": f"{job_id_log_prefix} {msg}"})
        
        # (MERGE) Logic này chỉ dùng cho v2 (Gantry)
        stop_conveyor = False
        use_gantry = False
        with state_lock:
            cfg_timing = system_state['timing_config']
            stop_conveyor = cfg_timing.get('stop_conveyor_on_entry', False)
            use_gantry = cfg_timing.get('use_sensor_entry_gantry', False)
        
        if use_gantry and stop_conveyor:
            qr_count = 0
            entry_count = 0
            with qr_queue_lock:
                qr_count = len(qr_queue)
            with processing_queue_lock:
                entry_count = len(processing_queue)
                
            if qr_count == 0 and entry_count == 0:
                 logging.info(f"[CONVEYOR] {job_id_log_prefix} Hoàn tất xử lý, không còn vật. Khởi động lại băng chuyền.")
                 CONVEYOR_RUN()
            else:
                 logging.info(f"[CONVEYOR] {job_id_log_prefix} Hoàn tất xử lý. Băng chuyền VẪN DỪNG (còn {qr_count} QR, {entry_count} vật).")

# =============================
# CÁC HÀM TEST RELAY
# =============================
test_seq_running = False
test_seq_lock = threading.Lock()

def _run_test_relay(lane_index, relay_action):
    push_pin, pull_pin, lane_name = None, None, f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                return broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane index {lane_index} không hợp lệ."})
            lane_state = system_state["lanes"][lane_index]
            lane_name = lane_state['name']
            push_pin = lane_state.get("push_pin"); pull_pin = lane_state.get("pull_pin")
            if push_pin is None and pull_pin is None:
                return broadcast_log({"log_type": "warn", "message": f"Lane '{lane_name}' là lane đi thẳng, không có relay."})
            if (push_pin is None or pull_pin is None):
                 return broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane '{lane_name}' thiếu pin PUSH hoặc PULL."})

        if relay_action == "push":
            broadcast_log({"log_type": "info", "message": f"Test: Kích hoạt ĐẨY (PUSH) cho '{lane_name}'."})
            RELAY_OFF(pull_pin); RELAY_ON(push_pin)
            with state_lock:
                if 0 <= lane_index < len(system_state["lanes"]):
                    system_state["lanes"][lane_index]["relay_grab"] = 0
                    system_state["lanes"][lane_index]["relay_push"] = 1
        
        elif relay_action == "grab":
            broadcast_log({"log_type": "info", "message": f"Test: Kích hoạt THU (PULL/GRAB) cho '{lane_name}'."})
            RELAY_OFF(push_pin); RELAY_ON(pull_pin)
            with state_lock:
                if 0 <= lane_index < len(system_state["lanes"]):
                    system_state["lanes"][lane_index]["relay_grab"] = 1
                    system_state["lanes"][lane_index]["relay_push"] = 0
    except Exception as e:
        logging.error(f"[TEST] Lỗi test relay '{relay_action}' cho '{lane_name}': {e}", exc_info=True)
        broadcast_log({"log_type": "error", "message": f"Lỗi test '{relay_action}' trên '{lane_name}': {e}"})
        reset_all_relays_to_default()

def _run_test_all_relays():
    global test_seq_running
    with test_seq_lock:
        if test_seq_running:
            return broadcast_log({"log_type": "warn", "message": "Test tuần tự đang chạy."})
        test_seq_running = True

    logging.info("[TEST] Bắt đầu test tuần tự (Cycle) relay...")
    broadcast_log({"log_type": "info", "message": "Bắt đầu test tuần tự (Cycle) relay..."})
    stopped_early = False

    try:
        num_lanes = 0
        cycle_delay, settle_delay = 0.3, 0.2
        with state_lock:
            num_lanes = len(system_state['lanes'])
            cfg = system_state['timing_config']
            cycle_delay = cfg.get('cycle_delay', 0.3)
            settle_delay = cfg.get('settle_delay', 0.2)

        for i in range(num_lanes):
            with test_seq_lock: stop_requested = not main_loop_running or not test_seq_running
            if stop_requested: stopped_early = True; break

            lane_name, push_pin, pull_pin = f"Lane {i+1}", None, None
            with state_lock:
                if 0 <= i < len(system_state['lanes']):
                    lane_state = system_state['lanes'][i]
                    lane_name = lane_state['name']
                    push_pin = lane_state.get("push_pin"); pull_pin = lane_state.get("pull_pin")
            
            if push_pin is None or pull_pin is None:
                broadcast_log({"log_type": "info", "message": f"Bỏ qua '{lane_name}' (lane đi thẳng)."})
                continue

            broadcast_log({"log_type": "info", "message": f"Testing Cycle cho '{lane_name}'..."})
            
            RELAY_OFF(pull_pin);
            with state_lock: system_state["lanes"][i]["relay_grab"] = 0
            time.sleep(settle_delay)
            if not main_loop_running or not test_seq_running: stopped_early = True; break

            RELAY_ON(push_pin);
            with state_lock: system_state["lanes"][i]["relay_push"] = 1
            time.sleep(cycle_delay)
            if not main_loop_running or not test_seq_running: stopped_early = True; break

            RELAY_OFF(push_pin);
            with state_lock: system_state["lanes"][i]["relay_push"] = 0
            time.sleep(settle_delay)
            if not main_loop_running or not test_seq_running: stopped_early = True; break

            RELAY_ON(pull_pin)
            with state_lock: system_state["lanes"][i]["relay_grab"] = 1
            
            time.sleep(0.5)

        if stopped_early: broadcast_log({"log_type": "warn", "message": "Test tuần tự đã dừng."})
        else: broadcast_log({"log_type": "info", "message": "Test tuần tự hoàn tất."})
    finally:
        with test_seq_lock: test_seq_running = False
        reset_all_relays_to_default()

# =============================
#       QUÉT MÃ QR (LOGIC V2 - CHỈ QUÉT)
# =============================
# --- (MERGE) Đổi tên từ qr_detection_loop (v2) -> qr_scanner_thread
def qr_scanner_thread():
    cv2_detector = cv2.QRCodeDetector()
    last_qr, last_time = "", 0.0
    data = None 
    
    if PYZBAR_ENABLED:
        logging.info("[QR_SCAN] Thread QR Scanner (v2 Logic) started (Ưu tiên Pyzbar).")
    else:
        logging.info("[QR_SCAN] Thread QR Scanner (v2 Logic) started (Chỉ dùng CV2).")

    while main_loop_running:
        try:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance():
                time.sleep(0.2); continue
            
            LANE_MAP = {}
            with state_lock:
                LANE_MAP = {canon_id(lane.get("id")): idx 
                            for idx, lane in enumerate(system_state["lanes"]) if lane.get("id")}
            
            if not LANE_MAP: # Chờ config load
                time.sleep(0.5)
                continue

            frame_copy = None
            with frame_lock:
                if latest_frame is not None: frame_copy = latest_frame.copy()
            if frame_copy is None:
                time.sleep(0.1); continue

            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 10:
                time.sleep(0.1); continue

            data = None 
            qr_source = None 
            
            if PYZBAR_ENABLED:
                try:
                    decoded_objects = pyzbar.decode(gray_frame)
                    if decoded_objects:
                        raw_bytes = decoded_objects[0].data
                        try:
                            data = raw_bytes.decode('utf-8', errors='ignore').strip().strip('\x00')
                            if not data: data = None
                            else: qr_source = "Pyzbar" 
                        except Exception: data = None
                except Exception: pass

            if data is None: 
                try:
                    data_cv2, _, _ = cv2_detector.detectAndDecode(gray_frame)
                    if data_cv2:
                        data = data_cv2.strip().strip('\x00') 
                        if not data: data = None
                        else: qr_source = "CV2" 
                except cv2.error:
                    data = None; time.sleep(0.1); continue
            
            if data and (data != last_qr or time.time() - last_time > 3.0):
                last_qr, last_time = data, time.time()
                data_key = canon_id(data); data_raw = data; now = time.time()

                if data_key in LANE_MAP:
                    idx = LANE_MAP[data_key]
                    current_queue_for_log = []
                    
                    with qr_queue_lock: 
                        qr_queue.append(idx) 
                        current_queue_for_log = list(qr_queue) 
                    
                    broadcast_log({
                        "log_type": "qr", 
                        "data": {"data_raw": data_raw, "data_key": data_key, "source": qr_source}
                    })
                    logging.info(f"[QR_SCAN] ({qr_source}) Hợp lệ: canon='{data_key}' -> lane {idx}. (Hàng chờ QR Tạm size={len(current_queue_for_log)})")
                            
                elif data_key == "NG":
                    broadcast_log({"log_type": "qr_ng", "data": data_raw, "source": qr_source})
                else:
                    broadcast_log({
                        "log_type": "unknown_qr", 
                        "data": {"data_raw": data_raw, "data_key": data_key, "source": qr_source}
                    }) 
                    logging.warning(f"[QR_SCAN] ({qr_source}) Không rõ mã QR: raw='{data_raw}', canon='{data_key}', keys={list(LANE_MAP.keys())}")
            
            time.sleep(0.1) 

        except Exception as e:
            logging.error(f"[QR_SCAN] Lỗi trong luồng QR Scanner: {e}", exc_info=True)
            time.sleep(0.5)

# =============================
#       HÀM THỰC THI AI
# =============================
# [NÂNG CẤP YOLOv8] Nâng cấp hàm để hỗ trợ YOLOv8 nâng cao với param conf, iou, augment, half
def run_ai_detection(ng_lane_index):
    if not AI_ENABLED or AI_MODEL is None:
        return ng_lane_index, None, None  # lane_index, class_name, track_id

    frame_copy = None
    with frame_lock:
        if latest_frame is not None:
            frame_copy = latest_frame.copy()
            
    if frame_copy is None:
        logging.warning("[AI] Không có frame camera để nhận diện.")
        return ng_lane_index, None, None

    try:
        # [NÂNG CẤP YOLOv8] Lấy config để tùy chỉnh param predict
        ai_cfg = {}
        with state_lock:
            ai_cfg = system_state.get('ai_config', {})
        
        predict_params = {
            'conf': ai_cfg.get('min_confidence', 0.6),
            'iou': ai_cfg.get('yolo_iou', 0.45),  # [NÂNG CẤP YOLOv8] Ngưỡng NMS
            'max_det': 5,  # [NÂNG CẤP YOLOv8] Cho phép nhiều detections
            'augment': ai_cfg.get('yolo_augment', False),  # [NÂNG CẤP YOLOv8] Augmentation cho độ chính xác
            'half': ai_cfg.get('yolo_half', False),  # [NÂNG CẤP YOLOv8] FP16 cho tốc độ
            'verbose': False
        }
        results = AI_MODEL.predict(frame_copy, **predict_params)
        
        if not results or len(results) == 0:
            return ng_lane_index, None, None

        result = results[0]
        if len(result.boxes) == 0:
            return ng_lane_index, None, None

        # [NÂNG CẤP DEEPSORT & YOLOv8] Chuẩn bị detections cho DeepSORT (format [bbox, conf, cls])
        detections = []
        for box in result.boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            conf = float(box.conf[0])
            cls = int(box.cls[0])
            detections.append(([x1, y1, x2 - x1, y2 - y1], conf, cls))  # [NÂNG CẤP YOLOv8] Format chuẩn cho DeepSORT

        track_id = None
        if DEEPSORT_TRACKER is not None and ai_cfg.get('enable_deepsort', False):
            tracks = DEEPSORT_TRACKER.update_tracks(detections, frame=frame_copy)
            if tracks:
                best_track = max(tracks, key=lambda t: t.get_det_conf() or 0)  # Chọn track tốt nhất dựa trên conf
                if best_track.is_confirmed():
                    track_id = best_track.track_id
                    logging.info(f"[DEEPSORT] Theo dõi ID {track_id} với conf {best_track.get_det_conf()}")

        # [NÂNG CẤP YOLOv8] Lọc detections và chọn best box
        high_conf_mask = result.boxes.conf > ai_cfg.get('min_confidence', 0.6)
        filtered_boxes = result.boxes[high_conf_mask]
        if len(filtered_boxes) == 0:
            logging.info("[AI] Không có phát hiện nào vượt ngưỡng confidence.")
            return ng_lane_index, None, None

        best_idx = filtered_boxes.conf.argmax()
        confidence = float(filtered_boxes.conf[best_idx])
        class_id = int(filtered_boxes.cls[best_idx])
        class_name = result.names[class_id].upper()

        if class_name in AI_LANE_MAP:
            lane_index = AI_LANE_MAP[class_name]
            logging.info(f"[AI] Phát hiện tốt nhất: '{class_name}' (Conf: {confidence:.2f}) -> Lane {lane_index} (Track ID: {track_id if track_id else 'N/A'})")
            return lane_index, class_name, track_id
        else:
            logging.warning(f"[AI] Phát hiện '{class_name}' nhưng class này chưa được map trong ai_config.")
            return ng_lane_index, None, None

    except Exception as e:
        logging.error(f"[AI] Lỗi trong lúc chạy model.predict: {e}", exc_info=True)
        return ng_lane_index, None, None

# =============================
#       HÀM HỖ TRỢ BĂNG CHUYỀN
# =============================
def restart_conveyor_after_delay(delay_seconds):
    try:
        time.sleep(delay_seconds)
        logging.info(f"[CONVEYOR] Hết thời gian {delay_seconds}s. Tự động KHỞI ĐỘNG băng chuyền.")
        CONVEYOR_RUN()
    except Exception as e:
        logging.error(f"[CONVEYOR] Lỗi trong luồng tự khởi động lại: {e}")

# =============================
# (CẬP NHẬT) LUỒNG TẠO JOB (LOGIC V1 - CAMERA TRIGGER) - ĐÃ SỬA LỖI LẶP MÃ
# =============================
def camera_trigger_job_creator_thread():
    cv2_detector = cv2.QRCodeDetector()
    last_qr, last_time = None, 0.0 # (CẬP NHẬT) Đổi last_qr thành None
    data = None 
    
    if PYZBAR_ENABLED:
        logging.info("[CAM_TRIG] Thread Camera Trigger (v1 Logic) started (Ưu tiên Pyzbar).")
    else:
        logging.info("[CAM_TRIG] Thread Camera Trigger (v1 Logic) started (Chỉ dùng CV2).")
        
    NG_LANE_INDEX = -1
    NG_LANE_NAME = "Hàng NG"
    with state_lock:
        for i, lane in enumerate(system_state["lanes"]):
            if canon_id(lane.get("id")) == "NG":
                NG_LANE_INDEX = i
                NG_LANE_NAME = lane.get("name", "Hàng NG")
                break
    logging.info(f"[CAM_TRIG] Đã cấu hình hàng NG tại index: {NG_LANE_INDEX} ({NG_LANE_NAME})")

    while main_loop_running:
        try:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance():
                time.sleep(0.2); continue
            
            LANE_MAP = {}
            stop_on_qr = False
            stop_delay_qr = 2.0
            ai_cfg = {}
            qr_debounce_time = 3.0 # (CẬP NHẬT)
            
            with state_lock:
                LANE_MAP = {canon_id(lane.get("id")): idx 
                            for idx, lane in enumerate(system_state["lanes"]) if lane.get("id")}
                cfg_timing = system_state.get('timing_config', {})
                stop_on_qr = cfg_timing.get('stop_conveyor_on_qr', False)
                stop_delay_qr = cfg_timing.get('conveyor_stop_delay_qr', 2.0)
                qr_debounce_time = cfg_timing.get('qr_debounce_time', 3.0) # (CẬP NHẬT)
                if qr_debounce_time < 1.0: qr_debounce_time = 1.0 # (CẬP NHẬT)
                ai_cfg = system_state.get('ai_config', {})

            if not LANE_MAP: 
                time.sleep(0.5)
                continue

            frame_copy = None
            with frame_lock:
                if latest_frame is not None: frame_copy = latest_frame.copy()
            if frame_copy is None:
                time.sleep(0.1); continue

            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 10:
                time.sleep(0.1); continue

            data = None 
            qr_source = None 
            
            if PYZBAR_ENABLED:
                try:
                    decoded_objects = pyzbar.decode(gray_frame)
                    if decoded_objects:
                        raw_bytes = decoded_objects[0].data
                        try:
                            data = raw_bytes.decode('utf-8', errors='ignore').strip().strip('\x00')
                            if not data: data = None
                            else: qr_source = "Pyzbar" 
                        except Exception: data = None
                except Exception: pass

            if data is None: 
                try:
                    data_cv2, _, _ = cv2_detector.detectAndDecode(gray_frame)
                    if data_cv2:
                        data = data_cv2.strip().strip('\x00') 
                        if not data: data = None
                        else: qr_source = "CV2" 
                except cv2.error:
                    data = None; time.sleep(0.1); continue
            
            # ==================================
            # (CẬP NHẬT) LOGIC DEBOUNCE MỚI TỪ APPV2.PY
            # ==================================
            now = time.time()

            if data:
                # Nếu phát hiện mã QR
                if data != last_qr:
                    # --- KÍCH HOẠT (TRIGGER) ---
                    # Đây là một mã QR MỚI (khác với mã trước đó).
                    # Đây là hành động kích hoạt duy nhất.
                    last_qr, last_time = data, now
                    data_key = canon_id(data); data_raw = data
                    
                    logging.info(f"[CAM_TRIG] Phát hiện mã MỚI: {data_raw}")

                    if data_key in LANE_MAP:
                        
                        ai_is_on = ai_cfg.get('enable_ai', False) and AI_ENABLED
                        ai_has_priority = ai_cfg.get('ai_priority', False)
                        
                        job_lane_index = NG_LANE_INDEX
                        job_lane_name = NG_LANE_NAME
                        job_status = "PENDING"
                        job_track_id = None  # [NÂNG CẤP DEEPSORT] Thêm track_id
                        
                        qr_lane_index = LANE_MAP[data_key] # Đây là lane từ QR
                        
                        ai_lane_index = NG_LANE_INDEX
                        ai_class_name = None
                        ai_track_id = None  # [NÂNG CẤP DEEPSORT]
                        if ai_is_on:
                            ai_lane_index, ai_class_name, ai_track_id = run_ai_detection(NG_LANE_INDEX)  # [NÂNG CẤP DEEPSORT] Thêm track_id

                        if ai_has_priority and ai_is_on:
                            if ai_lane_index != NG_LANE_INDEX:
                                job_lane_index = ai_lane_index
                                job_status = f"AI_MATCHED ({ai_class_name})"
                                job_track_id = ai_track_id
                            elif qr_lane_index is not None:
                                job_lane_index = qr_lane_index
                                job_status = "QR_MATCHED (AI_Fallback)"
                            else:
                                job_status = "ALL_FAILED" # Sẽ không xảy ra vì qr_lane_index luôn có
                        else:
                            if qr_lane_index is not None:
                                job_lane_index = qr_lane_index
                                job_status = "QR_MATCHED"
                            elif ai_is_on and ai_lane_index != NG_LANE_INDEX:
                                job_lane_index = ai_lane_index
                                job_status = f"AI_MATCHED ({ai_class_name}) (QR_Fallback)"
                                job_track_id = ai_track_id
                            else:
                                job_status = "ALL_FAILED"
                        
                        job_id = str(uuid.uuid4())[:8] 
                        job_id_log_prefix = f"[JobID {job_id}]"

                        job = {
                            "job_id": job_id, 
                            "lane_index": job_lane_index,
                            "status": job_status,
                            "entry_time": now,
                            "track_id": job_track_id  # [NÂNG CẤP DEEPSORT] Thêm track_id
                        }

                        if job_lane_index != NG_LANE_INDEX:
                            with state_lock:
                                if 0 <= job_lane_index < len(system_state["lanes"]):
                                    job_lane_name = system_state["lanes"][job_lane_index]["name"]
                                    system_state["lanes"][job_lane_index]["status"] = "Đang chờ vật..."
                        else:
                            job_lane_name = NG_LANE_NAME
                        
                        current_queue_indices = []
                        current_queue_len = 0 # (CẬP NHẬT)
                        with processing_queue_lock:
                            processing_queue.append(job)
                            if len(processing_queue) == 1:
                                queue_head_since = now
                            current_queue_len = len(processing_queue)
                            current_queue_indices = [j["lane_index"] for j in processing_queue]
                        
                        with state_lock:
                            system_state["queue_indices"] = current_queue_indices
                            system_state["entry_queue_size"] = current_queue_len
                        
                        broadcast_log({"log_type": "info", "message": f"{job_id_log_prefix} Vật vào Camera (QR). Ghép cặp: {job_status} -> Lane '{job_lane_name}' (Track ID: {job_track_id if job_track_id else 'N/A'})."})
                        logging.info(f"[CAM_TRIG] {job_id_log_prefix} Phát hiện QR. Ghép cặp: {job_status} -> Lane '{job_lane_name}'. Queue chính: {current_queue_len}")

                        if stop_on_qr:
                            logging.info(f"[CONVEYOR] {job_id_log_prefix} Phát hiện QR, DỪNG băng chuyền trong {stop_delay_qr}s...")
                            CONVEYOR_STOP()
                            executor.submit(restart_conveyor_after_delay, stop_delay_qr)
                elif data == last_qr and (now - last_time) < qr_debounce_time:
                    # --- DEBOUNCE (BỎ QUA) ---
                    # Đây là mã QR CŨ, và chưa hết debounce time.
                    # Không tạo job mới, chỉ log debug nếu cần.
                    pass
                else:
                    # --- RESET ---
                    # Hết debounce time, reset để sẵn sàng cho mã QR mới.
                    last_qr = None
            
            time.sleep(0.1) # Giảm tải CPU

        except Exception as e:
            logging.error(f"[CAM_TRIG] Lỗi trong luồng Camera Trigger: {e}", exc_info=True)
            time.sleep(0.5)

# =============================
#       LUỒNG TẠO JOB (LOGIC V2 - SENSOR GANTRY)
# =============================
# --- (MERGE) Đổi tên từ entry_sensor_monitoring_thread -> gantry_trigger_job_creator_thread
def gantry_trigger_job_creator_thread():
    global last_entry_sensor_state, last_entry_sensor_trigger_time, queue_head_since

    sensor_pin_to_read = SENSOR_ENTRY_PIN
    if isinstance(GPIO, MockGPIO):
        sensor_pin_to_read = SENSOR_ENTRY_MOCK_PIN
        
    logging.info(f"[GANTRY] Thread Gantry Trigger (v2 Logic) (Pin: {sensor_pin_to_read}) bắt đầu.")
    
    NG_LANE_INDEX = -1
    NG_LANE_NAME = "Hàng NG"
    with state_lock:
        for i, lane in enumerate(system_state["lanes"]):
            if canon_id(lane.get("id")) == "NG":
                NG_LANE_INDEX = i
                NG_LANE_NAME = lane.get("name", "Hàng NG")
                break
    logging.info(f"[GANTRY] Đã cấu hình hàng NG tại index: {NG_LANE_INDEX} ({NG_LANE_NAME})")

    # --- (BUG 1 FIX) Thêm cờ 'is_first_loop' để chống trigger lỗi khi khởi động
    is_first_loop = True

    while main_loop_running:
        if AUTO_TEST_ENABLED or error_manager.is_maintenance():
            time.sleep(0.1); continue
        
        ai_cfg = {}
        debounce_time = 0.1
        stop_conveyor_enabled = False
        conveyor_stop_delay = 1.0
        stability_delay = 0.25 

        with state_lock:
            cfg_timing = system_state['timing_config']
            debounce_time = cfg_timing.get('sensor_debounce', 0.1) 
            stability_delay = cfg_timing.get('stability_delay', stability_delay) 
            stop_conveyor_enabled = cfg_timing.get('stop_conveyor_on_entry', False)
            conveyor_stop_delay = cfg_timing.get('conveyor_stop_delay', 1.0)
            ai_cfg = system_state.get('ai_config', {})

        ai_is_on = ai_cfg.get('enable_ai', False) and AI_ENABLED
        ai_has_priority = ai_cfg.get('ai_priority', False)
        now = time.time()

        try:
            sensor_now = GPIO.input(sensor_pin_to_read)
        except Exception as gpio_e:
            logging.error(f"[GANTRY] Lỗi đọc GPIO pin {sensor_pin_to_read} (SENSOR_ENTRY): {gpio_e}")
            error_manager.trigger_maintenance(f"Lỗi đọc sensor ENTRY pin {sensor_pin_to_read}: {gpio_e}")
            time.sleep(0.5); continue

        with state_lock:
            system_state["sensor_entry_reading"] = sensor_now
        
        # --- (BUG 1 FIX) Logic "priming"
        # Gán trạng thái ban đầu và bỏ qua vòng lặp đầu tiên
        if is_first_loop:
            last_entry_sensor_state = sensor_now
            is_first_loop = False
            logging.info(f"[GANTRY] Đã 'priming' sensor gác cổng, trạng thái ban đầu: {'ACTIVE' if sensor_now == 0 else 'INACTIVE'}")
            time.sleep(0.1)
            continue
        # --- (HẾT BUG 1 FIX) ---

        if sensor_now == 0 and last_entry_sensor_state == 1: # Cạnh xuống (Kích hoạt)
            if (now - last_entry_sensor_trigger_time) > debounce_time:
                
                if stability_delay > 0:
                    time.sleep(stability_delay) 
                    if GPIO.input(sensor_pin_to_read) != 0: 
                        logging.info(f"[GANTRY] Bỏ qua nhiễu tạm thời (tay/quét nhanh) (dưới {stability_delay}s)")
                        last_entry_sensor_state = 1 
                        continue 
                
                last_entry_sensor_trigger_time = now
                
                job_lane_index = NG_LANE_INDEX
                job_lane_name = NG_LANE_NAME
                job_status = "PENDING"
                job_track_id = None  # [NÂNG CẤP DEEPSORT] Thêm track_id

                qr_lane_index = None
                try:
                    with qr_queue_lock:
                        qr_lane_index = qr_queue.pop(0)
                except IndexError:
                    pass

                ai_lane_index = NG_LANE_INDEX
                ai_class_name = None
                ai_track_id = None  # [NÂNG CẤP DEEPSORT]
                if ai_is_on:
                    ai_lane_index, ai_class_name, ai_track_id = run_ai_detection(NG_LANE_INDEX)

                if ai_has_priority and ai_is_on:
                    if ai_lane_index != NG_LANE_INDEX:
                        job_lane_index = ai_lane_index
                        job_status = f"AI_MATCHED ({ai_class_name})"
                        job_track_id = ai_track_id
                    elif qr_lane_index is not None:
                        job_lane_index = qr_lane_index
                        job_status = "QR_MATCHED (AI_Fallback)"
                    else:
                        job_status = "ALL_FAILED"
                else:
                    if qr_lane_index is not None:
                        job_lane_index = qr_lane_index
                        job_status = "QR_MATCHED"
                    elif ai_is_on and ai_lane_index != NG_LANE_INDEX:
                        job_lane_index = ai_lane_index
                        job_status = f"AI_MATCHED ({ai_class_name}) (QR_Fallback)"
                        job_track_id = ai_track_id
                    else:
                        job_status = "ALL_FAILED"
                
                job_id = str(uuid.uuid4())[:8] 
                job_id_log_prefix = f"[JobID {job_id}]"

                job = {
                    "job_id": job_id, 
                    "lane_index": job_lane_index,
                    "status": job_status,
                    "entry_time": now,
                    "track_id": job_track_id  # [NÂNG CẤP DEEPSORT] Thêm track_id
                }

                if job_lane_index != NG_LANE_INDEX:
                    with state_lock:
                        if 0 <= job_lane_index < len(system_state["lanes"]):
                            job_lane_name = system_state["lanes"][job_lane_index]["name"]
                            system_state["lanes"][job_lane_index]["status"] = "Đang chờ vật..."
                else:
                    job_lane_name = NG_LANE_NAME
                
                current_queue_indices = []
                with processing_queue_lock:
                    processing_queue.append(job)
                    if len(processing_queue) == 1:
                        queue_head_since = now
                    current_queue_len = len(processing_queue)
                    current_queue_indices = [j["lane_index"] for j in processing_queue]
                
                with state_lock:
                    system_state["queue_indices"] = current_queue_indices
                    system_state["entry_queue_size"] = current_queue_len
                
                broadcast_log({"log_type": "info", "message": f"{job_id_log_prefix} Vật vào Gác Cổng (Ổn định {stability_delay}s). Ghép cặp: {job_status} -> Lane '{job_lane_name}' (Track ID: {job_track_id if job_track_id else 'N/A'})."})
                logging.info(f"[GANTRY] {job_id_log_prefix} SENSOR_ENTRY kích hoạt. Ghép cặp: {job_status} -> Lane '{job_lane_name}'. Queue chính: {current_queue_len}")

                if stop_conveyor_enabled and job_status == "ALL_FAILED":
                    logging.warning(f"[GANTRY] {job_id_log_prefix} Đọc QR và AI đều thất bại, DỪNG băng chuyền...")
                    CONVEYOR_STOP()
                    executor.submit(restart_conveyor_after_delay, conveyor_stop_delay)

        last_entry_sensor_state = sensor_now
        time.sleep(0.05)

# =============================
#       LUỒNG SENSOR LÀN (TIÊU THỤ JOB)
# =============================
# --- (MERGE) Lấy từ v2.py, nhưng thêm logic Sửa Bug 2
def lane_sensor_monitoring_thread():
    global last_sensor_state, last_sensor_trigger_time
    global queue_head_since
    
    # --- (BUG 2 FIX) Thêm biến cho logic Auto-Test (lấy từ v1)
    global auto_test_last_state, auto_test_last_trigger
    
    last_sensor_state_prev = list(last_sensor_state) 

    NG_LANE_INDEX = -1
    NG_LANE_NAME = "Hàng NG"
    try:
        with state_lock:
            for i_ng, lane_ng in enumerate(system_state["lanes"]):
                if canon_id(lane_ng.get("id")) == "NG":
                    NG_LANE_INDEX = i_ng
                    NG_LANE_NAME = lane_ng.get("name", "Hàng NG")
                    break
        logging.info(f"[LANE_S] Đã cấu hình hàng NG tại index: {NG_LANE_INDEX}")
    except Exception as e:
        logging.error(f"[LANE_S] Lỗi khi tìm NG Lane Index: {e}")

    try:
        while main_loop_running:
            # --- (BUG 2 FIX) Thêm logic Auto-Test
            if error_manager.is_maintenance():
                time.sleep(0.1); continue

            # Lấy config (cần cho cả 2 chế độ)
            debounce_time, current_queue_timeout, num_lanes = 0.1, 15.0, 0
            with state_lock:
                cfg_timing = system_state['timing_config']
                debounce_time = cfg_timing.get('sensor_debounce', 0.1)
                current_queue_timeout = cfg_timing.get('queue_head_timeout', 15.0)
                num_lanes = len(system_state['lanes'])
            now = time.time()
            
            # --- (BUG 2 FIX) LOGIC AUTO TEST (Lấy từ v1) ---
            if AUTO_TEST_ENABLED:
                
                if len(auto_test_last_state) != num_lanes:
                    auto_test_last_state = [1] * num_lanes
                    auto_test_last_trigger = [0.0] * num_lanes
                    logging.warning(f"[AUTO-TEST] Đã đồng bộ kích thước mảng auto_test (size {num_lanes}).")

                for i in range(num_lanes):
                    sensor_pin, push_pin, pull_pin, lane_name_for_log = None, None, None, "UNKNOWN"
                    with state_lock:
                        if not (0 <= i < len(system_state["lanes"])): continue
                        lane_for_read = system_state["lanes"][i]
                        sensor_pin = lane_for_read.get("sensor_pin")
                        push_pin = lane_for_read.get("push_pin")
                        pull_pin = lane_for_read.get("pull_pin")
                        lane_name_for_log = lane_for_read['name']

                    if sensor_pin is None or (push_pin is None or pull_pin is None):
                        continue 
                    # (MERGE) Bỏ qua sensor gantry nếu nó nằm trong list lane
                    if (sensor_pin == SENSOR_ENTRY_PIN) or (isinstance(GPIO, MockGPIO) and sensor_pin == SENSOR_ENTRY_MOCK_PIN):
                        continue

                    try:
                        sensor_now = GPIO.input(sensor_pin)
                    except Exception as gpio_e:
                        logging.error(f"[AUTO-TEST] Lỗi đọc GPIO pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                        error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                        continue
                    
                    with state_lock:
                        if 0 <= i < len(system_state["lanes"]):
                            system_state["lanes"][i]["sensor_reading"] = sensor_now

                    prev_state = auto_test_last_state[i]
                    
                    if sensor_now == 0 and prev_state == 1: # Cạnh xuống (mới kích hoạt)
                        if (now - auto_test_last_trigger[i]) > debounce_time:
                            auto_test_last_trigger[i] = now
                            broadcast_log({"log_type": "info", "message": f"[Auto-Test] Kích hoạt {lane_name_for_log}!"})
                            threading.Thread(target=sorting_process, args=(i, "AUTO-TEST"), daemon=True).start()
                    
                    auto_test_last_state[i] = sensor_now

                time.sleep(0.02) 
                continue # Bỏ qua logic queue bên dưới
            
            # --- (HẾT BUG 2 FIX) ---
            
            # --- LOGIC CHÍNH (QUEUE) ---
            
            if len(last_sensor_state_prev) != num_lanes:
                with state_lock:
                    reference_state = [lane['sensor_reading'] for lane in system_state['lanes']]
                
                if len(reference_state) < num_lanes:
                    reference_state.extend([1] * (num_lanes - len(reference_state)))
                elif len(reference_state) > num_lanes:
                    reference_state = reference_state[:num_lanes]
                
                last_sensor_state_prev = reference_state
                logging.warning(f"[SENSOR] Đã phát hiện thay đổi config. Đồng bộ last_sensor_state_prev (size {num_lanes}).")


            current_queue_indices = []
            with processing_queue_lock:
                current_queue_indices = [j["lane_index"] for j in processing_queue]
                if processing_queue and queue_head_since > 0.0:
                    if (now - queue_head_since) > current_queue_timeout:
                        job_timeout = processing_queue.pop(0) 
                        job_id_timeout = job_timeout.get('job_id', '???') 
                        expected_lane_index = job_timeout['lane_index']
                        expected_lane_name = "UNKNOWN"
                        current_queue_indices = [j["lane_index"] for j in processing_queue]
                        
                        with state_lock:
                            if 0 <= expected_lane_index < len(system_state["lanes"]):
                                expected_lane_name = system_state['lanes'][expected_lane_index]['name']
                                if system_state["lanes"][expected_lane_index]["status"].startswith("Đang chờ vật"):
                                    system_state["lanes"][expected_lane_index]["status"] = "Sẵn sàng"
                            system_state["queue_indices"] = current_queue_indices
                            system_state["entry_queue_size"] = len(current_queue_indices)

                        queue_head_since = now if processing_queue else 0.0

                        broadcast_log({
                            "log_type": "warn",
                            "message": f"[JobID {job_id_timeout}] TIMEOUT! Đã tự động xóa Job cho {expected_lane_name} (>{current_queue_timeout}s).",
                            "queue": current_queue_indices
                        })
                        logging.warning(f"[SENSOR] [JobID {job_id_timeout}] TIMEOUT! Xóa Job cho {expected_lane_name}.")
                        
            for i in range(num_lanes):
                sensor_pin, push_pin, lane_name_for_log = None, None, "UNKNOWN"
                with state_lock:
                    if not (0 <= i < len(system_state["lanes"])): continue
                    lane_for_read = system_state["lanes"][i]
                    sensor_pin = lane_for_read.get("sensor_pin")
                    push_pin = lane_for_read.get("push_pin")
                    lane_name_for_log = lane_for_read['name']

                if sensor_pin is None: continue
                # (MERGE) Bỏ qua sensor gantry nếu nó nằm trong list lane
                if (sensor_pin == SENSOR_ENTRY_PIN) or (isinstance(GPIO, MockGPIO) and sensor_pin == SENSOR_ENTRY_MOCK_PIN):
                    continue

                try:
                    sensor_now = GPIO.input(sensor_pin)
                except Exception as gpio_e:
                    logging.error(f"[SENSOR] Lỗi đọc GPIO pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    continue

                with state_lock:
                    if 0 <= i < len(system_state["lanes"]):
                        system_state["lanes"][i]["sensor_reading"] = sensor_now

                prev_state = last_sensor_state_prev[i] if i < len(last_sensor_state_prev) else 1

                if sensor_now == 0 and prev_state == 1:
                    if (now - last_sensor_trigger_time[i]) > debounce_time:
                        last_sensor_trigger_time[i] = now

                        job_to_run = None
                        is_head_match = False
                        
                        with processing_queue_lock:
                            current_queue_indices_for_log = [j["lane_index"] for j in processing_queue]

                            while processing_queue:
                                job_head = processing_queue[0]
                                job_id_head = job_head.get('job_id', '???')

                                if job_head["lane_index"] == i:
                                    is_head_match = True
                                    job_to_run = processing_queue.pop(0)
                                    queue_head_since = now if processing_queue else 0.0
                                    break 

                                elif job_head["lane_index"] == NG_LANE_INDEX:
                                    job_ng_removed = processing_queue.pop(0)
                                    queue_head_since = now if processing_queue else 0.0
                                    current_queue_indices_for_log = [j["lane_index"] for j in processing_queue]
                                    
                                    logging.info(f"[SENSOR] [JobID {job_id_head}] {lane_name_for_log} kích hoạt. Tự động 'tiêu thụ' 1 Job NG khỏi hàng chờ.")
                                    broadcast_log({"log_type": "info", "message": f"[JobID {job_id_head}] Vật NG đã đi thẳng (pass-through). Xóa Job NG.", "queue": current_queue_indices_for_log})
                                    continue 

                                else:
                                    is_head_match = False
                                    job_to_run = None
                                    logging.warning(f"[SENSOR] ⚠️ [JobID {job_id_head}] {lane_name_for_log} kích hoạt nhưng KHÔNG KHỚP Job đầu hàng chờ (Lane {job_head['lane_index']}). Bỏ qua.")
                                    broadcast_log({"log_type": "warn", "message": f"Sensor {lane_name_for_log} kích hoạt (lỗi đồng bộ). Bỏ qua.", "queue": current_queue_indices_for_log})
                                    break
                        
                        if is_head_match and job_to_run:
                            job_id_for_log = job_to_run.get('job_id', 'N/A') 
                            
                            with processing_queue_lock:
                                current_queue_indices = [j["lane_index"] for j in processing_queue]

                            with state_lock:
                                system_state["queue_indices"] = current_queue_indices
                                system_state["entry_queue_size"] = len(current_queue_indices)
                                if 0 <= i < len(system_state["lanes"]):
                                    lane_ref = system_state["lanes"][i]
                                    if push_pin is None: lane_ref["status"] = "Đang đi thẳng..."
                                    else: lane_ref["status"] = "Đang chờ đẩy"
                            
                            threading.Thread(target=sorting_process, args=(i, job_id_for_log), daemon=True).start()
                            
                            broadcast_log({"log_type": "info", "message": f"[JobID {job_id_for_log}] Sensor {lane_name_for_log} khớp Job. Bắt đầu xử lý.", "queue": current_queue_indices})
                            logging.info(f"[LANE_S] [JobID {job_id_for_log}] {lane_name_for_log} kích hoạt. KHỚP Job. Queue chính: {len(current_queue_indices)}")
                        
                        else:
                            pass
                
                if i < len(last_sensor_state_prev):
                    last_sensor_state_prev[i] = sensor_now
            
            # (MERGE) Sleep dựa trên trạng thái của tất cả sensor (bao gồm cả gantry)
            adaptive_sleep = 0.05 if all(s == 1 for s in last_sensor_state_prev) and last_entry_sensor_state == 1 else 0.01
            time.sleep(adaptive_sleep)

    except Exception as e:
        logging.error(f"[ERROR] Luồng lane_sensor_monitoring_thread bị crash: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi luồng Lane Sensor: {e}")

# =============================
#     FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
connected_clients = set()
clients_lock = threading.Lock()
broadcast_lock = threading.Lock() 

def _add_client(ws):
    with clients_lock: connected_clients.add(ws)
def _remove_client(ws):
    with clients_lock: connected_clients.discard(ws)
def _list_clients():
    with clients_lock: return list(connected_clients)

def broadcast_log(log_data):
    log_data['timestamp'] = time.strftime('%H:%M:%S')
    msg = json.dumps({"type": "log", **log_data})
    
    clients_to_send = _list_clients()
    if not clients_to_send: return

    with broadcast_lock: 
        for client in clients_to_send:
            try: client.send(msg)
            except Exception: _remove_client(client)

# =============================
#     CÁC HÀM CỦA FLASK
# =============================
def check_auth(username, password):
    if not AUTH_ENABLED: return True
    return username == USERNAME and password == PASSWORD
def authenticate():
    return Response('Yêu cầu đăng nhập.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
def requires_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_ENABLED:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

def vps_update_thread():
    global latest_frame, system_state, state_lock, frame_lock
    
    time.sleep(5)
    logging.info(f"[VPS_UPDATE] Bắt đầu luồng gửi dữ liệu (Color 20FPS) lên {VPS_URL}")
    
    session = requests.Session()
    headers = {'X-API-Key': PI_API_KEY}
    
    while main_loop_running:
        try:
            state_copy = None
            frame_copy = None
            
            with state_lock:
                state_copy = copy.deepcopy(system_state)
            with frame_lock:
                if latest_frame is not None:
                    frame_copy = latest_frame.copy()
            
            if state_copy is None or frame_copy is None:
                logging.warning("[VPS_UPDATE] Chưa có state hoặc frame, chờ 1s...")
                time.sleep(1)
                continue

            state_json = json.dumps(state_copy)
            
            # === (TỐI ƯU) ===
            # 1. KHÔNG chuyển sang đen trắng, giữ nguyên ảnh màu (frame_copy)
            # 2. Nén ảnh MÀU với chất lượng 70 (cao hơn)
            ret, buffer = cv2.imencode('.jpg', frame_copy, [cv2.IMWRITE_JPEG_QUALITY, 70])
            # === (HẾT TỐI ƯU) ===
            
            if not ret:
                logging.warning("[VPS_UPDATE] Lỗi encode JPEG")
                time.sleep(0.5)
                continue
            
            frame_bytes = buffer.tobytes()

            files = {'frame': ('frame.jpg', frame_bytes, 'image/jpeg')}
            data = {'state': state_json}
            
            response = session.post(
                f"{VPS_URL}/api/pi/update",
                files=files,
                data=data,
                headers=headers,
                timeout=2.0
            )
            
            if response.status_code != 200:
                logging.warning(f"[VPS_UPDATE] VPS báo lỗi: {response.status_code} - {response.text}")

            # (TỐI ƯU) Giữ nguyên 20 FPS (ngủ 0.05s)
            time.sleep(0.05) 
            
        except requests.exceptions.RequestException as e:
            logging.error(f"[VPS_UPDATE] Lỗi kết nối VPS: {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"[VPS_UPDATE] Lỗi trong luồng: {e}")
            time.sleep(1)
def broadcast_state():
    last_state_str = ""
    while main_loop_running:
        state_copy_for_json = None 
        
        queue_len = 0
        current_queue_indices = []
        with processing_queue_lock:
            queue_len = len(processing_queue)
            current_queue_indices = [j["lane_index"] for j in processing_queue]
            
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["is_mock"] = isinstance(GPIO, MockGPIO)
            system_state["auth_enabled"] = AUTH_ENABLED
            system_state["gpio_mode"] = system_state['timing_config'].get('gpio_mode', 'BOARD')
            system_state["entry_queue_size"] = queue_len
            system_state["queue_indices"] = current_queue_indices
            # (MERGE) Phải cập nhật sensor_entry_reading từ biến global (nếu logic v2 chạy)
            system_state["sensor_entry_reading"] = last_entry_sensor_state 
            
            try:
                state_copy_for_json = copy.deepcopy(system_state)
            except Exception as e:
                logging.warning(f"[BROADCAST] Lỗi khi deepcopy state: {e}")
                time.sleep(0.5)
                continue
        
        current_msg = ""
        try:
            current_msg = json.dumps({"type": "state_update", "state": state_copy_for_json})
        except Exception as e:
                logging.warning(f"[BROADCAST] Lỗi JSON encode state: {e}")
                time.sleep(0.5)
                continue
        
        clients_to_send = _list_clients()
        if not clients_to_send:
            time.sleep(0.5)
            continue

        if current_msg != last_state_str:
            with broadcast_lock:
                for client in clients_to_send:
                    try: client.send(current_msg)
                    except Exception: _remove_client(client)
            last_state_str = current_msg
        
        time.sleep(0.5)

def generate_frames():
    while main_loop_running:
        frame = None
        if not error_manager.is_maintenance():
            with frame_lock:
                if latest_frame is not None:
                    frame = latest_frame.copy()
        
        if frame is None:
            import numpy as np
            frame = np.zeros((480, 640, 3), dtype=np.uint8)
            time.sleep(0.1)
        
        try:
            fps_text = f"FPS: {fps_value:.2f}"
            color = (0, 255, 255) if error_manager.is_maintenance() else (0, 128, 0)
            cv2.putText(frame, fps_text, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2, cv2.LINE_AA)
        except Exception as e:
            logging.warning(f"[FRAME] Lỗi khi vẽ FPS: {e}")

        try:
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            yield (b'--frame\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        except Exception as encode_e:
            logging.error(f"[CAMERA] Lỗi encode frame: {encode_e}")
            import numpy as np
            frame = np.zeros((480, 640, 3), dtype=np.uint8)
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 10])
            yield (b'--frame\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
            
        time.sleep(1 / 30) # Stream 30 FPS

# --- Các routes (endpoints) ---

@app.route('/')
@requires_auth
def index():
    # (MERGE) Đổi tên file HTML
    return render_template('indexv2.html')

@app.route('/video_feed')
@requires_auth
def video_feed():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/config')
@requires_auth
def get_config():
    with state_lock:
        config_data = {
            "timing_config": system_state.get('timing_config', {}).copy(),
            "ai_config": system_state.get('ai_config', {}).copy(),
            "camera_settings": system_state.get('camera_settings', {}).copy(),
            "lanes_config": [{
                "id": ln.get('id'), "name": ln.get('name'),
                "sensor_pin": ln.get('sensor_pin'), "push_pin": ln.get('push_pin'),
                "pull_pin": ln.get('pull_pin')
             } for ln in system_state.get('lanes', [])]
        }
    return jsonify(config_data)

@app.route('/api/sort_log')
@requires_auth
def get_sort_log():
    output_data = {} 
    
    with database_lock:
        try:
            conn = sqlite3.connect(DATABASE_FILE)
            cursor = conn.cursor()
            
            cursor.execute("SELECT date, lane_name, count FROM sort_log ORDER BY date ASC")
            rows = cursor.fetchall()
            conn.close()
            
            for date, lane_name, count in rows:
                if date not in output_data:
                    output_data[date] = {}
                output_data[date][lane_name] = count
                
        except Exception as e:
            logging.error(f"[API] Lỗi khi đọc /api/sort_log từ SQLite: {e}")
            return jsonify({"error": str(e)}), 500
            
    return jsonify(output_data)


@app.route('/update_config', methods=['POST'])
@requires_auth
def update_config():
    global lanes_config, RELAY_PINS, SENSOR_PINS, RELAY_CONVEYOR_PIN
    global QUEUE_HEAD_TIMEOUT
    global last_sensor_state, last_sensor_trigger_time, auto_test_last_state, auto_test_last_trigger
    global DEEPSORT_TRACKER  # [NÂNG CẤP DEEPSORT] Cập nhật nếu config thay đổi

    new_config_data = request.json
    if not new_config_data:
        return jsonify({"error": "Thiếu dữ liệu JSON"}), 400
    logging.info(f"[CONFIG] Nhận config mới từ API (POST): {new_config_data}")

    # (MERGE) Nhận config từ 1 nguồn duy nhất
    new_timing_config = new_config_data.get('timing_config', {})
    new_lanes_config = new_config_data.get('lanes_config')
    new_ai_config = new_config_data.get('ai_config')
    new_camera_settings = new_config_data.get('camera_settings')

    config_to_save = {}
    restart_required = False

    with state_lock:
        current_ai_config = system_state.get('ai_config', {})
        if new_ai_config is not None and new_ai_config != current_ai_config:
            logging.warning("[CONFIG] Cài đặt AI đã thay đổi. Cần khởi động lại.")
            broadcast_log({"log_type": "warn", "message": "Cài đặt AI đã đổi. Cần khởi động lại!"})
            current_ai_config.update(new_ai_config)
            system_state['ai_config'] = current_ai_config
            restart_required = True
            # [NÂNG CẤP DEEPSORT] Khởi tạo lại tracker nếu thay đổi
            if current_ai_config.get('enable_deepsort', False) and DEEPSORT_ENABLED:
                DEEPSORT_TRACKER = DeepSort(
                    max_age=current_ai_config.get('deepsort_max_age', 30),
                    n_init=current_ai_config.get('deepsort_n_init', 3),
                    max_iou_distance=current_ai_config.get('deepsort_max_iou_distance', 0.7),  # [NÂNG CẤP YOLOv8] Cập nhật param
                    nms_max_overlap=1.0,
                    max_cosine_distance=0.3,
                    nn_budget=None,
                    embedder="mobilenet_v2"
                )
        config_to_save['ai_config'] = current_ai_config.copy()
        
        current_camera_settings = system_state.get('camera_settings', {})
        if new_camera_settings is not None and new_camera_settings != current_camera_settings:
            logging.warning("[CONFIG] Cài đặt Camera đã thay đổi. Cần khởi động lại.")
            broadcast_log({"log_type": "warn", "message": "Cài đặt Camera đã đổi. Cần khởi động lại!"})
            current_camera_settings.update(new_camera_settings)
            system_state['camera_settings'] = current_camera_settings
            restart_required = True
        config_to_save['camera_settings'] = current_camera_settings.copy()

        current_timing = system_state['timing_config']
        current_gpio_mode = current_timing.get('gpio_mode', 'BOARD')
        
        default_timing = { 
            "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
            "push_delay": 0.0, "gpio_mode": "BOARD",
            "queue_head_timeout": 15.0, "pending_trigger_timeout": 0.5,
            "RELAY_CONVEYOR_PIN": None, "stop_conveyor_on_entry": False,
            "stability_delay": 0.25,
            "stop_conveyor_on_qr": False,
            "conveyor_stop_delay_qr": 2.0,
            "qr_debounce_time": 3.0, # (CẬP NHẬT) Thêm default cho qr_debounce_time
            "use_sensor_entry_gantry": False # (MERGE) Thêm default
        }
        temp_timing = default_timing.copy()
        temp_timing.update(current_timing); temp_timing.update(new_timing_config)
        current_timing = temp_timing
        system_state['timing_config'] = current_timing
        
        QUEUE_HEAD_TIMEOUT = current_timing.get('queue_head_timeout', 15.0)
        
        new_conveyor_pin = current_timing.get('RELAY_CONVEYOR_PIN')
        if new_conveyor_pin != RELAY_CONVEYOR_PIN:
            logging.warning("[CONFIG] Chân Relay Băng chuyền đã thay đổi. Cần khởi động lại.")
            RELAY_CONVEYOR_PIN = new_conveyor_pin
            restart_required = True
        
        # (MERGE) Log thêm cờ gantry
        logging.info(f"[CONFIG] Đã cập nhật động: Queue Timeout={QUEUE_HEAD_TIMEOUT}s")
        logging.info(f"[CONFIG] Đã cập nhật động: Stop Conveyor (Entry)={current_timing.get('stop_conveyor_on_entry')}")
        logging.info(f"[CONFIG] Đã cập nhật động: Stop Conveyor (QR)={current_timing.get('stop_conveyor_on_qr')}")
        logging.info(f"[CONFIG] Đã cập nhật động: Stability Delay={current_timing.get('stability_delay')}")
        logging.info(f"[CONFIG] Đã cập nhật động: Use Gantry={current_timing.get('use_sensor_entry_gantry')}")
        if current_timing.get('use_sensor_entry_gantry') != system_state['timing_config'].get('use_sensor_entry_gantry'):
            restart_required = True # Thay đổi logic cần restart

        
        new_gpio_mode = new_timing_config.get('gpio_mode', current_gpio_mode)
        if new_gpio_mode != current_gpio_mode:
            logging.warning("[CONFIG] Chế độ GPIO đã thay đổi. Cần khởi động lại ứng dụng.")
            broadcast_log({"log_type": "warn", "message": "GPIO Mode đã đổi. Cần khởi động lại!"})
            restart_required = True
        config_to_save['timing_config'] = current_timing.copy()

        if new_lanes_config is not None:
            logging.info("[CONFIG] Cập nhật cấu hình lanes...")
            lanes_config = ensure_lane_ids(new_lanes_config)
            num_lanes = len(lanes_config)
            new_system_lanes = []; new_relay_pins = []; new_sensor_pins = []

            # (MERGE) Thêm pin gantry
            if SENSOR_ENTRY_PIN: new_sensor_pins.append(SENSOR_ENTRY_PIN)
            if isinstance(GPIO, MockGPIO) and SENSOR_ENTRY_MOCK_PIN: new_sensor_pins.append(SENSOR_ENTRY_MOCK_PIN)
            if RELAY_CONVEYOR_PIN: new_relay_pins.append(RELAY_CONVEYOR_PIN)

            for i, lane_cfg in enumerate(lanes_config):
                new_system_lanes.append({
                    "name": lane_cfg.get("name", f"Lane {i+1}"), "id": lane_cfg.get("id"),
                    "status": "Sẵn sàng", "count": 0, 
                    "sensor_pin": lane_cfg.get("sensor_pin"), "push_pin": lane_cfg.get("push_pin"),
                    "pull_pin": lane_cfg.get("pull_pin"), "sensor_reading": 1,
                    "relay_grab": 0, "relay_push": 0
                })
                if lane_cfg.get("sensor_pin") is not None: new_sensor_pins.append(lane_cfg["sensor_pin"])
                if lane_cfg.get("push_pin") is not None: new_relay_pins.append(lane_cfg["push_pin"])
                if lane_cfg.get("pull_pin") is not None: new_relay_pins.append(lane_cfg["pull_pin"])
            
            system_state['lanes'] = new_system_lanes
            
            # (MERGE) Thêm state auto-test
            last_sensor_state = [1] * num_lanes; last_sensor_trigger_time = [0.0] * num_lanes
            auto_test_last_state = [1] * num_lanes; auto_test_last_trigger = [0.0] * num_lanes
            
            RELAY_PINS, SENSOR_PINS = new_relay_pins, new_sensor_pins
            config_to_save['lanes_config'] = lanes_config
            restart_required = True
            logging.warning("[CONFIG] Cấu hình lanes đã thay đổi. Cần khởi động lại ứng dụng.")
            broadcast_log({"log_type": "warn", "message": "Cấu hình Lanes đã đổi. Cần khởi động lại!"})
        else:
            config_to_save['lanes_config'] = [
                {"id": l.get('id'), "name": l['name'], "sensor_pin": l.get('sensor_pin'),
                 "push_pin": l.get('push_pin'), "pull_pin": l.get('pull_pin')}
                for l in system_state['lanes']
            ]

    try:
        with config_file_lock:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config_to_save, f, indent=4)
        
        msg = "Đã lưu config. "
        if restart_required: msg += "Vui lòng khởi động lại hệ thống để áp dụng thay đổi."
        else: msg += "Các thay đổi về timing đã được áp dụng."
        logging.info(f"[CONFIG] {msg}")
        broadcast_log({"log_type": "info", "message": msg})
        
        return jsonify({"message": msg, "config": config_to_save, "restart_required": restart_required})

    except Exception as e:
        logging.error(f"[ERROR] Không thể lưu config (POST): {e}")
        broadcast_log({"log_type": "error", "message": f"Lỗi khi lưu config (POST): {e}"})
        return jsonify({"error": str(e)}), 500

@app.route('/api/reset_maintenance', methods=['POST'])
@requires_auth
def reset_maintenance():
    global queue_head_since, last_entry_sensor_state, last_entry_sensor_trigger_time

    if error_manager.is_maintenance():
        error_manager.reset()
        
        with qr_queue_lock:
            qr_queue.clear()
        with processing_queue_lock:
            processing_queue.clear()
            queue_head_since = 0.0
        
        last_entry_sensor_state = 1
        last_entry_sensor_trigger_time = 0.0

        with state_lock:
            system_state["queue_indices"] = []
            system_state["entry_queue_size"] = 0
            system_state["sensor_entry_reading"] = 1
            
        broadcast_log({"log_type": "success", "message": "Chế độ bảo trì đã được reset. Hàng chờ đã được xóa."})
        return jsonify({"message": "Maintenance mode reset thành công."})
    else:
        return jsonify({"message": "Hệ thống không ở chế độ bảo trì."})

@app.route('/api/queue/reset', methods=['POST'])
@requires_auth
def api_queue_reset():
    global queue_head_since

    if error_manager.is_maintenance():
        return jsonify({"error": "Hệ thống đang bảo trì, không thể reset hàng chờ."}), 403
    try:
        with qr_queue_lock:
            qr_queue.clear()
        with processing_queue_lock:
            processing_queue.clear()
            queue_head_since = 0.0
            current_queue_for_log = []

        with state_lock:
            for lane in system_state["lanes"]:
                lane["status"] = "Sẵn sàng"
            system_state["queue_indices"] = current_queue_for_log
            system_state["entry_queue_size"] = 0
            
        broadcast_log({"log_type": "warn", "message": "Tất cả hàng chờ (Tạm & Chính) đã được reset thủ công.", "queue": current_queue_for_log})
        logging.info("[API] Tất cả hàng chờ đã được reset thủ công.")
        return jsonify({"message": "Hàng chờ đã được reset."})
    except Exception as e:
        logging.error(f"[API] Lỗi khi reset hàng chờ: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mock_gpio', methods=['POST'])
@requires_auth
def api_mock_gpio():
    if not isinstance(GPIO, MockGPIO):
        return jsonify({"error": "Chức năng chỉ khả dụng ở chế độ mô phỏng."}), 400
    
    payload = request.get_json(silent=True) or {}; lane_index = payload.get('lane_index')
    pin = payload.get('pin'); requested_state = payload.get('state')
    
    pin_to_mock = None
    lane_name = "N/A"

    if lane_index is not None and pin is None:
        try: lane_index = int(lane_index)
        except (TypeError, ValueError): return jsonify({"error": "lane_index không hợp lệ."}), 400
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']):
                pin_to_mock = system_state['lanes'][lane_index].get('sensor_pin')
                lane_name = system_state['lanes'][lane_index].get('name', lane_name)
            else: return jsonify({"error": "lane_index vượt ngoài phạm vi."}), 400
    
    elif pin is not None:
        try: pin_to_mock = int(pin)
        except (TypeError, ValueError): return jsonify({"error": "Giá trị pin không hợp lệ."}), 400
        
        if pin_to_mock == SENSOR_ENTRY_PIN:
            pin_to_mock = SENSOR_ENTRY_MOCK_PIN
            lane_name = "SENSOR_ENTRY (Real Pin)"
        elif pin_to_mock == SENSOR_ENTRY_MOCK_PIN:
            lane_name = "SENSOR_ENTRY (Mock Pin)"
        else:
            with state_lock:
                 for lane in system_state['lanes']:
                    if lane.get('sensor_pin') == pin_to_mock:
                        lane_name = lane.get('name', f"Pin {pin_to_mock}")
                        break

    if pin_to_mock is None: return jsonify({"error": "Thiếu thông tin chân sensor."}), 400

    if requested_state is None: logical_state = GPIO.toggle_input_state(pin_to_mock)
    else:
        logical_state = 1 if str(requested_state).strip().lower() in {"1", "true", "high", "inactive"} else 0
        GPIO.set_input_state(pin_to_mock, logical_state)

    with state_lock:
        if pin_to_mock == SENSOR_ENTRY_MOCK_PIN:
            system_state['sensor_entry_reading'] = 0 if logical_state == 0 else 1
            global last_entry_sensor_state # (MERGE) Cập nhật biến global
            last_entry_sensor_state = system_state['sensor_entry_reading']
        else:
            for lane in system_state['lanes']:
                if lane.get('sensor_pin') == pin_to_mock:
                    lane['sensor_reading'] = 0 if logical_state == 0 else 1
                    break
                    
    state_label = 'ACTIVE (LOW)' if logical_state == 0 else 'INACTIVE (HIGH)'
    message = f"[MOCK] Sensor pin {pin_to_mock} -> {state_label} ({lane_name})";
    
    broadcast_log({"log_type": "info", "message": message})
    return jsonify({"pin": pin_to_mock, "state": logical_state, "lane": lane_name})

# =============================
#     WEBSOCKET
# =============================
@sock.route('/ws')
@requires_auth
def ws_route(ws):
    global AUTO_TEST_ENABLED, test_seq_running, queue_head_since, last_entry_sensor_state, last_entry_sensor_trigger_time
    
    auth_user = "guest";
    if AUTH_ENABLED:
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            logging.warning("[WS] Unauthorized connection attempt.")
            ws.close(code=1008, reason="Unauthorized"); return
        auth_user = auth.username
    client_label = f"{auth_user}-{id(ws):x}"
    _add_client(ws)
    logging.info(f"[WS] Client {client_label} connected. Total: {len(_list_clients())}")
    
    queue_len = 0
    current_queue_indices = []
    with processing_queue_lock:
        queue_len = len(processing_queue)
        current_queue_indices = [j["lane_index"] for j in processing_queue]
    try:
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["auth_enabled"] = AUTH_ENABLED
            system_state["entry_queue_size"] = queue_len
            system_state["sensor_entry_reading"] = last_entry_sensor_state
            system_state["queue_indices"] = current_queue_indices
            initial_state_msg = json.dumps({"type": "state_update", "state": system_state})
        ws.send(initial_state_msg)
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}")
        _remove_client(ws); return

    try:
        while True:
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    action = data.get('action')
                    if error_manager.is_maintenance() and action != "reset_maintenance":
                        broadcast_log({"log_type": "error", "message": "Hệ thống đang bảo trì, không thể thao tác."})
                        continue

                    if action == 'reset_count':
                        lane_idx = data.get('lane_index')
                        with state_lock:
                            if lane_idx == 'all':
                                for i in range(len(system_state['lanes'])): system_state['lanes'][i]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"{client_label} đã reset đếm toàn bộ."})
                            elif lane_idx is not None and 0 <= lane_idx < len(system_state['lanes']):
                                lane_name = system_state['lanes'][lane_idx]['name']
                                system_state['lanes'][lane_idx]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"{client_label} đã reset đếm {lane_name}."})

                    elif action == "test_relay":
                        lane_index = data.get("lane_index"); relay_action = data.get("relay_action")
                        if lane_index is not None and relay_action:
                            executor.submit(_run_test_relay, lane_index, relay_action)
                    elif action == "test_all_relays":
                        executor.submit(_run_test_all_relays)
                    elif action == "toggle_auto_test":
                        AUTO_TEST_ENABLED = data.get("enabled", False)
                        logging.info(f"[TEST] Auto-Test (Sensor->Relay) set by {client_label} to: {AUTO_TEST_ENABLED}")
                        broadcast_log({"log_type": "warn", "message": f"Chế độ Auto-Test đã { 'BẬT' if AUTO_TEST_ENABLED else 'TẮT' } bởi {client_label}."})
                        if not AUTO_TEST_ENABLED: reset_all_relays_to_default()
                    
                    elif action == "reset_maintenance":
                        if error_manager.is_maintenance():
                            error_manager.reset()
                            with qr_queue_lock:
                                qr_queue.clear()
                            with processing_queue_lock:
                                processing_queue.clear()
                                queue_head_since = 0.0
                            
                            last_entry_sensor_state = 1
                            last_entry_sensor_trigger_time = 0.0
                                
                            with state_lock:
                                system_state["queue_indices"] = []
                                system_state["entry_queue_size"] = 0
                                system_state["sensor_entry_reading"] = 1
                                
                            broadcast_log({"log_type": "success", "message": f"Chế độ bảo trì đã được reset bởi {client_label}. Hàng chờ đã được xóa."})
                        else:
                            broadcast_log({"log_type": "info", "message": "Hệ thống không ở chế độ bảo trì."})

                except json.JSONDecodeError: pass
                except Exception as ws_loop_e: logging.error(f"[WS] Lỗi xử lý message: {ws_loop_e}")
    except Exception as ws_conn_e:
        logging.warning(f"[WS] Kết nối WebSocket bị đóng hoặc lỗi: {ws_conn_e}")
    finally:
        _remove_client(ws)
        logging.info(f"[WS] Client {client_label} disconnected. Total: {len(_list_clients())}")
        
# =============================
#             MAIN
# =============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hệ thống phân loại sản phẩm với YOLOv8 nâng cao và DeepSORT.")
    parser.add_argument('--test', action='store_true', help="Chạy ví dụ kiểm tra mã.")
    args = parser.parse_args()

    if args.test:
        class TestYOLOv8DeepSORT(unittest.TestCase):
            def test_import_deepsort(self):
                if DEEPSORT_ENABLED:
                    from deep_sort_realtime.deepsort_tracker import DeepSort
                    self.assertTrue(True)
                else:
                    self.fail("DeepSORT không được import.")

            def test_load_yolo(self):
                if YOLO_ENABLED:
                    try:
                        model = YOLO("yolov8n.pt")  # Giả lập, thay bằng model_path thực nếu có
                        self.assertTrue(True)
                    except Exception as e:
                        self.fail(f"Lỗi tải YOLOv8: {e}")
                else:
                    self.fail("YOLO không được bật.")

            def test_run_ai_detection(self):
                if AI_ENABLED:
                    # Frame giả (đen 640x480)
                    fake_frame = np.zeros((480, 640, 3), dtype=np.uint8)
                    lane_idx, class_name, track_id = run_ai_detection(0)  # NG_LANE_INDEX giả = 0
                    self.assertIsNotNone(lane_idx)  # Kiểm tra không crash
                else:
                    self.skipTest("AI không được bật.")

        unittest.main()
    else:
        try:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
                                handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()])
            
            init_database()
            load_local_config()
            load_queues_on_startup() 
            
            loaded_gpio_mode = ""
            use_gantry_logic = False
            with state_lock:
                loaded_gpio_mode = system_state.get("gpio_mode", "BOARD")
                use_gantry_logic = system_state['timing_config'].get('use_sensor_entry_gantry', False)

            if isinstance(GPIO, RealGPIO):
                mode_to_set = GPIO.BOARD if loaded_gpio_mode == "BOARD" else GPIO.BCM
                GPIO.setmode(mode_to_set); GPIO.setwarnings(False)
                logging.info(f"[GPIO] Đã đặt chế độ chân cắm là: {loaded_gpio_mode}")
                
                active_sensor_pins = list(set([pin for pin in SENSOR_PINS if pin is not None]))
                active_relay_pins = list(set([pin for pin in RELAY_PINS if pin is not None]))
                
                logging.info(f"[GPIO] Setup SENSOR pins: {active_sensor_pins}")
                for pin in active_sensor_pins:
                    try: GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                    except Exception as e:
                        logging.critical(f"[CRITICAL] Lỗi cấu hình chân SENSOR {pin}: {e}.")
                        error_manager.trigger_maintenance(f"Lỗi cấu hình chân SENSOR {pin}: {e}")
                        raise
                logging.info(f"[GPIO] Setup RELAY pins: {active_relay_pins}")
                for pin in active_relay_pins:
                    try: GPIO.setup(pin, GPIO.OUT)
                    except Exception as e:
                        logging.critical(f"[CRITICAL] Lỗi cấu hình chân RELAY {pin}: {e}.")
                        error_manager.trigger_maintenance(f"Lỗi cấu hình chân RELAY {pin}: {e}")
                        raise
            else:
                logging.info("[GPIO] Chạy ở chế độ Mock, bỏ qua setup vật lý.")

            reset_all_relays_to_default()

            # Khởi động các luồng chung
            threading.Thread(target=camera_capture_thread, name="CameraThread", daemon=True).start()
            threading.Thread(target=lane_sensor_monitoring_thread, name="LaneSensorThread", daemon=True).start() # (Đã fix Bug 2)
            threading.Thread(target=broadcast_state, name="BroadcastThread", daemon=True).start()
            threading.Thread(target=periodic_config_save, name="ConfigSaveThread", daemon=True).start()
            threading.Thread(target=vps_update_thread, name="VPSUpdateThread", daemon=True).start()
            # --- (MERGE) Khởi động luồng logic có điều kiện ---
            if use_gantry_logic:
                logging.info("[MAIN] Đang khởi động ở chế độ: Sensor Gantry (v2 Logic).")
                threading.Thread(target=gantry_trigger_job_creator_thread, name="GantryTriggerThread", daemon=True).start() # (Đã fix Bug 1)
                threading.Thread(target=qr_scanner_thread, name="QRScannerThread", daemon=True).start()
            else:
                logging.info("[MAIN] Đang khởi động ở chế độ: Camera Trigger (v1 Logic).")
                # (CẬP NHẬT) Gọi hàm đã được vá lỗi
                threading.Thread(target=camera_trigger_job_creator_thread, name="CameraTriggerThread", daemon=True).start()
                

            logging.info("=========================================")
            logging.info("    HỆ THỐNG PHÂN LOẠI SẴN SÀNG (MERGED & FIXED & YOLOv8 NÂNG CAO)")
            if use_gantry_logic:
                logging.info("    Logic: Sensor Gantry (v2)")
            else:
                logging.info("    Logic: Camera Trigger (v1) - ĐÃ SỬA LỖI LẶP MÃ") # (CẬP NHẬT)
            logging.info(f"    GPIO Mode: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} (Config: {loaded_gpio_mode})")
            logging.info(f"    API State: http://<IP_CUA_PI>:3000")
            if AUTH_ENABLED:
                logging.info(f"    Truy cập: http://<IP_CUA_PI>:3000 (User: {USERNAME} / Pass: {PASSWORD})")
            else:
                logging.info("    Truy cập: http://<IP_CUA_PI>:3000 (KHÔNG yêu cầu đăng nhập)")
            logging.info("=========================================")
            
            app.run(host='0.0.0.0', port=3000, debug=False)

        except KeyboardInterrupt:
            logging.info("\n🛑 Dừng hệ thống (Ctrl+C)...")
        except Exception as main_e:
            logging.critical(f"[CRITICAL] Lỗi khởi động hệ thống: {main_e}", exc_info=True)
            try:
                if isinstance(GPIO, RealGPIO): GPIO.cleanup()
            except Exception: pass
        finally:
            main_loop_running = False
            
            save_queues_on_shutdown() 
            
            logging.info("Đang tắt ThreadPoolExecutor...")
            executor.shutdown(wait=False)
            logging.info("Đang cleanup GPIO...")
            try:
                GPIO.cleanup()
                logging.info("✅ GPIO cleaned up.")
            except Exception as clean_e:
                logging.warning(f"Lỗi khi cleanup GPIO: {clean_e}")
            logging.info("👋 Tạm biệt!")