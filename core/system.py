# pi/core/system.py
import cv2
import time
import json
import os
import logging
import threading
import sqlite3
import copy
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Import các thành phần cốt lõi
from .gpio import get_gpio_provider, GPIOProvider, MockGPIO, RealGPIO
from .ai import AIDetector, YOLO_AVAILABLE, DEEPSORT_AVAILABLE
from .qr import scan_qr_from_frame
from .utils import canon_id


# Import các luồng (threads)
from threads import (
    camera, lane, gantry, qr_scanner, camera_trigger,
    vps, broadcast, config_save, test_utils
)

# Thư mục và đường dẫn (Lấy từ app_god.py)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
LOG_DIR = os.path.join(BASE_DIR, "logs")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
DATABASE_FILE = os.path.join(LOG_DIR, "sort_log.db")
QUEUE_STATE_FILE = os.path.join(LOG_DIR, "queue_state.json")

# Hằng số (Lấy từ app_god.py)
ACTIVE_LOW = True
SENSOR_ENTRY_PIN = 6
SENSOR_ENTRY_MOCK_PIN = 99

class ErrorManager:
    """Quản lý lỗi và chế độ bảo trì (Lấy từ app_god.py)"""
    def __init__(self, system_broadcaster):
        self.lock = threading.Lock()
        self.maintenance_mode = False
        self.last_error = None
        self.broadcast_log = system_broadcaster

    def trigger_maintenance(self, message):
        with self.lock:
            if self.maintenance_mode: return
            self.maintenance_mode = True
            self.last_error = message
            logging.critical("="*50 + f"\n[MAINTENANCE MODE] Lỗi nghiêm trọng: {message}\n" + "="*50)
            self.broadcast_log("error", f"MAINTENANCE MODE: {message}")

    def reset(self):
        with self.lock:
            self.maintenance_mode = False
            self.last_error = None
            logging.info("[MAINTENANCE MODE] Đã reset chế độ bảo trì.")

    def is_maintenance(self):
        return self.maintenance_mode

class SortingSystem:
    def __init__(self):
        logging.info("[SYSTEM] Khởi tạo SortingSystem...")
        self.main_loop_running = True
        self.gpio: GPIOProvider = get_gpio_provider()
        
        # Hàng chờ và Khóa (Locks)
        self.qr_queue = [] # (Dùng cho v2)
        self.processing_queue = [] # Hàng chờ chính
        self.qr_queue_lock = threading.Lock()
        self.processing_queue_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.frame_lock = threading.Lock()
        self.database_lock = threading.Lock()
        self.config_file_lock = threading.Lock()
        self.test_seq_lock = threading.Lock() # Cho test tuần tự

        # Quản lý WebSocket Clients
        self.ws_clients = set()
        self.ws_lock = threading.Lock()
        self.broadcast_lock = threading.Lock()

        # Trạng thái hệ thống (Lấy từ app_god.py)
        self.system_state = {
            "lanes": [], "timing_config": {}, "is_mock": isinstance(self.gpio, MockGPIO),
            "maintenance_mode": False, "auth_enabled": False, "gpio_mode": "BOARD",
            "last_error": None, "queue_indices": [], "sensor_entry_reading": 1,
            "entry_queue_size": 0, "ai_config": {}, "camera_settings": {}
        }
        
        self.error_manager = ErrorManager(self.broadcast_log)
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="SysWorker")

        # Trạng thái camera và AI
        self.latest_frame = None
        self.fps_value = 0.0
        self.ai_detector: Optional[AIDetector] = None # Dùng class AIDetector từ core/ai.py
        self.NG_LANE_INDEX = -1
        self.NG_LANE_NAME = "Hàng NG"

        # Trạng thái hàng chờ và sensor (Lấy từ app_god.py)
        self.queue_head_since = 0.0
        self.last_sensor_state = []
        self.last_sensor_trigger_time = []
        self.auto_test_enabled = False
        self.auto_test_last_state = []
        self.auto_test_last_trigger = []
        self.last_entry_sensor_state = 1
        self.last_entry_sensor_trigger_time = 0.0
        self.test_seq_running = False # Cho test tuần tự

        # Pins (sẽ được load từ config)
        self.RELAY_PINS = []
        self.SENSOR_PINS = []
        self.RELAY_CONVEYOR_PIN = None

        # Tải config ban đầu
        self._load_local_config()

    # ===========================================
    # CÁC HÀM QUẢN LÝ GPIO (Lấy từ app_god.py)
    # ===========================================
    def RELAY_ON(self, pin):
        if pin is not None:
            try: self.gpio.output(pin, self.gpio.LOW if ACTIVE_LOW else self.gpio.HIGH)
            except Exception as e:
                logging.error(f"[GPIO] Lỗi RELAY_ON pin {pin}: {e}")
                self.error_manager.trigger_maintenance(f"Lỗi GPIO pin {pin}: {e}")
                
    def RELAY_OFF(self, pin):
        if pin is not None:
            try: self.gpio.output(pin, self.gpio.HIGH if ACTIVE_LOW else self.gpio.LOW)
            except Exception as e:
                logging.error(f"[GPIO] Lỗi RELAY_OFF pin {pin}: {e}")
                self.error_manager.trigger_maintenance(f"Lỗi GPIO pin {pin}: {e}")

    def CONVEYOR_RUN(self):
        logging.info("[CONVEYOR] Băng chuyền: RUN")
        self.RELAY_ON(self.RELAY_CONVEYOR_PIN)

    def CONVEYOR_STOP(self):
        logging.info("[CONVEYOR] Băng chuyền: STOP")
        self.RELAY_OFF(self.RELAY_CONVEYOR_PIN)

    def reset_all_relays_to_default(self):
        logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT, BĂNG CHUYỀN CHẠY).")
        with self.state_lock:
            for lane in self.system_state["lanes"]:
                pull_pin = lane.get("pull_pin")
                push_pin = lane.get("push_pin")
                if pull_pin is not None: self.RELAY_ON(pull_pin)
                if push_pin is not None: self.RELAY_OFF(push_pin)
                lane["relay_grab"] = 1 if pull_pin is not None else 0
                lane["relay_push"] = 0
                lane["status"] = "Sẵn sàng"
        self.CONVEYOR_RUN()
        time.sleep(0.1)
        logging.info("[GPIO] Reset hoàn tất.")

    # ===========================================
    # CÁC HÀM LOGIC CỐT LÕI (Lấy từ app_god.py)
    # ===========================================

    def sorting_process(self, lane_index, job_id="N/A"): 
        """Chu trình đẩy/thả vật lý (Lấy từ app_god.py)"""
        job_id_log_prefix = f"[JobID {job_id}]"
        
        lane_name = ""; push_pin, pull_pin = None, None
        is_sorting_lane = False
        try:
            with self.state_lock:
                if not (0 <= lane_index < len(self.system_state["lanes"])):
                    logging.error(f"[SORT] {job_id_log_prefix} Lane index {lane_index} không hợp lệ.")
                    return
                cfg = self.system_state['timing_config']
                delay = cfg['cycle_delay']; settle_delay = cfg['settle_delay']
                lane = self.system_state["lanes"][lane_index]
                lane_name = lane["name"]; push_pin = lane.get("push_pin"); pull_pin = lane.get("pull_pin")
                is_sorting_lane = not (push_pin is None and pull_pin is None)
                if is_sorting_lane and (push_pin is None or pull_pin is None):
                    logging.error(f"[SORT] {job_id_log_prefix} Lane {lane_name} (index {lane_index}) chưa được cấu hình đủ chân relay.")
                    lane["status"] = "Lỗi Config"
                    self.broadcast_log("error", f"{job_id_log_prefix} Lane {lane_name} thiếu cấu hình chân relay.")
                    return
                lane["status"] = "Đang phân loại..." if is_sorting_lane else "Đang đi thẳng..."
            
            if not is_sorting_lane:
                self.broadcast_log("info", f"{job_id_log_prefix} Vật phẩm đi thẳng qua {lane_name}")
            if is_sorting_lane:
                self.broadcast_log("info", f"{job_id_log_prefix} Bắt đầu chu trình đẩy {lane_name}")
                self.RELAY_OFF(pull_pin)
                with self.state_lock: self.system_state["lanes"][lane_index]["relay_grab"] = 0
                time.sleep(settle_delay);
                if not self.main_loop_running: return
                self.RELAY_ON(push_pin)
                with self.state_lock: self.system_state["lanes"][lane_index]["relay_push"] = 1
                time.sleep(delay);
                if not self.main_loop_running: return
                self.RELAY_OFF(push_pin)
                with self.state_lock: self.system_state["lanes"][lane_index]["relay_push"] = 0
                time.sleep(settle_delay);
                if not self.main_loop_running: return
                self.RELAY_ON(pull_pin)
                with self.state_lock: self.system_state["lanes"][lane_index]["relay_grab"] = 1

        except Exception as e:
            logging.error(f"[SORT] {job_id_log_prefix} Lỗi trong sorting_process (lane {lane_name}): {e}")
            self.error_manager.trigger_maintenance(f"Lỗi sorting_process (Lane {lane_name}): {e}")
        finally:
            with self.state_lock:
                if 0 <= lane_index < len(self.system_state["lanes"]):
                    lane = self.system_state["lanes"][lane_index]
                    if lane_name and lane["status"] != "Lỗi Config":
                        lane["count"] += 1
                        log_type = "sort" if is_sorting_lane else "pass"
                        self.broadcast_log(log_type, "", data={"name": lane_name, "count": lane['count']})
                        self.log_sort_count(lane_index, lane_name)
                        if lane["status"] != "Lỗi Config":
                            lane["status"] = "Sẵn sàng"
            if lane_name:
                msg = f"Hoàn tất chu trình cho {lane_name}" if is_sorting_lane else f"Hoàn tất đếm vật phẩm đi thẳng qua {lane_name}"
                self.broadcast_log("info", f"{job_id_log_prefix} {msg}")
            
            stop_conveyor = False
            use_gantry = False
            with self.state_lock:
                cfg_timing = self.system_state['timing_config']
                stop_conveyor = cfg_timing.get('stop_conveyor_on_entry', False)
                use_gantry = cfg_timing.get('use_sensor_entry_gantry', False)
            
            if use_gantry and stop_conveyor:
                qr_count = 0; entry_count = 0
                with self.qr_queue_lock: qr_count = len(self.qr_queue)
                with self.processing_queue_lock: entry_count = len(self.processing_queue)
                    
                if qr_count == 0 and entry_count == 0:
                     logging.info(f"[CONVEYOR] {job_id_log_prefix} Hoàn tất xử lý, không còn vật. Khởi động lại băng chuyền.")
                     self.CONVEYOR_RUN()
                else:
                     logging.info(f"[CONVEYOR] {job_id_log_prefix} Hoàn tất xử lý. Băng chuyền VẪN DỪNG (còn {qr_count} QR, {entry_count} vật).")

    def run_ai_detection(self, ng_lane_index):
        """Thực thi AI (Lấy từ app_god.py, nhưng dùng class AIDetector)"""
        if not self.ai_detector or not self.ai_detector.enabled:
            return ng_lane_index, None, None

        frame_copy = None
        with self.frame_lock:
            if self.latest_frame is not None:
                frame_copy = self.latest_frame.copy()
                
        if frame_copy is None:
            logging.warning("[AI] Không có frame camera để nhận diện.")
            return ng_lane_index, None, None
        
        try:
            # AIDetector.detect đã bao gồm logic của YOLOv8 và DeepSORT
            lane_index, class_name, track_id = self.ai_detector.detect(frame_copy)
            
            if lane_index != -1:
                logging.info(f"[AI] Phát hiện: '{class_name}' -> Lane {lane_index} (Track ID: {track_id if track_id else 'N/A'})")
                return lane_index, class_name, track_id
            else:
                return ng_lane_index, None, None
                
        except Exception as e:
            logging.error(f"[AI] Lỗi trong lúc chạy model.predict: {e}", exc_info=True)
            return ng_lane_index, None, None

    def restart_conveyor_after_delay(self, delay_seconds):
        """Luồng phụ cho băng chuyền (Lấy từ app_god.py)"""
        try:
            time.sleep(delay_seconds)
            logging.info(f"[CONVEYOR] Hết thời gian {delay_seconds}s. Tự động KHỞI ĐỘNG băng chuyền.")
            self.CONVEYOR_RUN()
        except Exception as e:
            logging.error(f"[CONVEYOR] Lỗi trong luồng tự khởi động lại: {e}")

    # ===========================================
    # KHỞI TẠO & VÒNG LẶP CHÍNH
    # ===========================================
    
    def run(self):
        """Hàm này được gọi 1 LẦN DUY NHẤT bởi web/app.py để khởi động các luồng nền"""
        try:
            logging.info("[SYSTEM] Bắt đầu chạy các luồng nền...")
            self._init_database()
            self._load_queues_on_startup()
            self._setup_gpio() # Setup chân cắm
            self.reset_all_relays_to_default() # Reset vật lý

            # Xác định NG Lane
            with self.state_lock:
                for i, lane in enumerate(self.system_state["lanes"]):
                    if canon_id(lane.get("id")) == "NG":
                        self.NG_LANE_INDEX = i
                        self.NG_LANE_NAME = lane.get("name", "Hàng NG")
                        break
            logging.info(f"[SYSTEM] Đã cấu hình hàng NG tại index: {self.NG_LANE_INDEX} ({self.NG_LANE_NAME})")

            # Khởi động các luồng chung
            threading.Thread(target=camera.start_camera_thread, args=(self,), name="CameraThread", daemon=True).start()
            threading.Thread(target=lane.start_lane_monitor_thread, args=(self,), name="LaneSensorThread", daemon=True).start()
            threading.Thread(target=broadcast.start_broadcast_state_thread, args=(self,), name="BroadcastThread", daemon=True).start()
            threading.Thread(target=config_save.start_periodic_config_save_thread, args=(self,), name="ConfigSaveThread", daemon=True).start()
            threading.Thread(target=vps.start_vps_thread, args=(self,), name="VPSUpdateThread", daemon=True).start()
            
            # Khởi động luồng logic có điều kiện (v1 hoặc v2)
            use_gantry_logic = False
            with self.state_lock:
                use_gantry_logic = self.system_state['timing_config'].get('use_sensor_entry_gantry', False)

            if use_gantry_logic:
                logging.info("[MAIN] Đang khởi động ở chế độ: Sensor Gantry (v2 Logic).")
                threading.Thread(target=gantry.start_gantry_trigger_thread, args=(self,), name="GantryTriggerThread", daemon=True).start()
                threading.Thread(target=qr_scanner.start_qr_scanner_thread, args=(self,), name="QRScannerThread", daemon=True).start()
            else:
                logging.info("[MAIN] Đang khởi động ở chế độ: Camera Trigger (v1 Logic).")
                threading.Thread(target=camera_trigger.start_camera_trigger_thread, args=(self,), name="CameraTriggerThread", daemon=True).start()
                
            logging.info("[SYSTEM] Tất cả các luồng nền đã được khởi động.")
            
            # Giữ luồng này chạy (hoặc có thể kết thúc nếu các luồng con là daemon)
            while self.main_loop_running:
                time.sleep(1)

        except Exception as main_e:
            logging.critical(f"[CRITICAL] Lỗi nghiêm trọng trong system.run: {main_e}", exc_info=True)
            self.error_manager.trigger_maintenance(f"Lỗi vòng lặp chính: {main_e}")
        finally:
            self.stop()
            
    def stop(self):
        if not self.main_loop_running: return
        logging.info("\n🛑 [SHUTDOWN] Dừng hệ thống...")
        self.main_loop_running = False
        self.save_queues_on_shutdown()
        logging.info("[SHUTDOWN] Đang tắt ThreadPoolExecutor...")
        self.executor.shutdown(wait=False)
        logging.info("[SHUTDOWN] Đang cleanup GPIO...")
        try:
            self.gpio.cleanup()
            logging.info("✅ [SHUTDOWN] GPIO cleaned up.")
        except Exception as clean_e:
            logging.warning(f"[SHUTDOWN] Lỗi khi cleanup GPIO: {clean_e}")
        logging.info("👋 [SHUTDOWN] Tạm biệt!")

    # ===========================================
    # CÁC HÀM CONFIG VÀ DATABASE (Lấy từ app_god.py)
    # ===========================================
    
    def _init_database(self):
        with self.database_lock:
            try:
                conn = sqlite3.connect(DATABASE_FILE)
                cursor = conn.cursor()
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS sort_log (
                    date TEXT, lane_name TEXT, count INTEGER DEFAULT 0,
                    PRIMARY KEY (date, lane_name)
                )""")
                conn.commit(); conn.close()
                logging.info(f"[DB] Đã khởi tạo CSDL SQLite tại '{DATABASE_FILE}' thành công.")
            except Exception as e:
                logging.critical(f"[CRITICAL] Không thể khởi tạo CSDL SQLite: {e}")
                self.error_manager.trigger_maintenance(f"Lỗi khởi tạo CSDL SQLite: {e}")

    def log_sort_count(self, lane_index, lane_name):
        """Ghi log đếm vào CSDL (Lấy từ app_god.py)"""
        with self.database_lock:
            try:
                today = datetime.now().strftime('%Y-%m-%d')
                conn = sqlite3.connect(DATABASE_FILE)
                cursor = conn.cursor()
                cursor.execute("""
                INSERT INTO sort_log (date, lane_name, count) VALUES (?, ?, 1)
                ON CONFLICT(date, lane_name) DO UPDATE SET count = count + 1
                """, (today, lane_name))
                conn.commit(); conn.close()
            except Exception as e:
                logging.error(f"[DB] Lỗi khi ghi log đếm vào SQLite: {e}")

    def get_sort_log_data(self):
        """Lấy dữ liệu log cho API (Lấy từ app_god.py)"""
        output_data = {}
        with self.database_lock:
            try:
                conn = sqlite3.connect(DATABASE_FILE)
                cursor = conn.cursor()
                cursor.execute("SELECT date, lane_name, count FROM sort_log ORDER BY date ASC")
                rows = cursor.fetchall()
                conn.close()
                for date, lane_name, count in rows:
                    if date not in output_data: output_data[date] = {}
                    output_data[date][lane_name] = count
            except Exception as e:
                logging.error(f"[API] Lỗi khi đọc /api/sort_log từ SQLite: {e}")
                return {"error": str(e)}
        return output_data

    def _ensure_lane_ids(self, lanes_list):
        """(Lấy từ app_god.py)"""
        default_ids = ['SP001', 'SP002', 'SP003', 'SP004', 'SP005', 'SP006']
        for i, lane in enumerate(lanes_list):
            if 'id' not in lane or not lane['id']:
                lane['id'] = default_ids[i] if i < len(default_ids) else f"LANE_{i+1}"
                logging.warning(f"[CONFIG] Lane {i+1} thiếu ID. Đã gán ID: {lane['id']}")
        return lanes_list

    def _load_local_config(self):
        """Tải và áp dụng config (Lấy từ app_god.py)"""
        
        # Cấu hình mặc định
        default_ai_config = {
            "enable_ai": False, "ai_priority": False, "model_path": "yolov8n.pt",
            "min_confidence": 0.6, "yolo_iou": 0.45, "yolo_augment": False, "yolo_half": False,
            "enable_deepsort": True, "deepsort_max_age": 30, "deepsort_n_init": 3,
            "deepsort_max_iou_distance": 0.7,
            "ai_class_to_id_map": { "APPLE": "SP001", "ORANGE": "SP002" }
        }
        default_timing_config = {
            "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
            "push_delay": 0.0, "gpio_mode": "BOARD", "queue_head_timeout": 15.0,
            "pending_trigger_timeout": 0.5, "RELAY_CONVEYOR_PIN": None,
            "stop_conveyor_on_entry": False, "stability_delay": 0.25,
            "stop_conveyor_on_qr": False, "conveyor_stop_delay_qr": 2.0,
            "qr_debounce_time": 3.0, "use_sensor_entry_gantry": False
        }
        default_camera_settings = { "auto_exposure": False, "brightness": 128, "contrast": 32 }
        default_lanes_config = [
            {"id": "SP001", "name": "Phân loại A", "sensor_pin": 5, "push_pin": 11, "pull_pin": 12},
            {"id": "SP002", "name": "Phân loại B", "sensor_pin": 16, "push_pin": 13, "pull_pin": 8},
            {"id": "SP003", "name": "Phân loại C", "sensor_pin": 18, "push_pin": 15, "pull_pin": 7},
            {"id": "NG", "name": "Sản Phẩm NG(Bỏ)", "sensor_pin": None, "push_pin": None, "pull_pin": None},
        ]
        default_config_full = {
            "timing_config": default_timing_config,
            "lanes_config": default_lanes_config,
            "ai_config": default_ai_config,
            "camera_settings": default_camera_settings
        }
        
        loaded_config = default_config_full
        
        with self.config_file_lock:
            if os.path.exists(CONFIG_FILE):
                try:
                    with open(CONFIG_FILE, 'r', encoding='utf-8') as f: file_content = f.read()
                    if file_content:
                        loaded_config_from_file = json.loads(file_content)
                        
                        timing_cfg = default_timing_config.copy(); timing_cfg.update(loaded_config_from_file.get('timing_config', {})); loaded_config['timing_config'] = timing_cfg
                        ai_cfg = default_ai_config.copy(); ai_cfg.update(loaded_config_from_file.get('ai_config', {})); loaded_config['ai_config'] = ai_cfg
                        cam_cfg = default_camera_settings.copy(); cam_cfg.update(loaded_config_from_file.get('camera_settings', {})); loaded_config['camera_settings'] = cam_cfg
                        lanes_from_file = loaded_config_from_file.get('lanes_config', default_lanes_config)
                        loaded_config['lanes_config'] = self._ensure_lane_ids(lanes_from_file)
                except Exception as e:
                    logging.error(f"[CONFIG] Lỗi đọc/parse file config ({e}), dùng mặc định.")
                    self.error_manager.trigger_maintenance(f"Lỗi JSON file config.json: {e}")
                    loaded_config = default_config_full
            else:
                logging.warning("[CONFIG] Không có file config, dùng mặc định và tạo mới.")
                self._save_config_to_file(loaded_config) # Lưu file mặc định

        # Áp dụng config vào self.system_state và các biến
        lanes_config = loaded_config['lanes_config']
        num_lanes = len(lanes_config)
        new_system_lanes = []
        self.RELAY_PINS = []; self.SENSOR_PINS = []
        
        if SENSOR_ENTRY_PIN: self.SENSOR_PINS.append(SENSOR_ENTRY_PIN)
        if isinstance(self.gpio, MockGPIO) and SENSOR_ENTRY_MOCK_PIN:
            self.SENSOR_PINS.append(SENSOR_ENTRY_MOCK_PIN)
            
        self.RELAY_CONVEYOR_PIN = loaded_config['timing_config'].get('RELAY_CONVEYOR_PIN')
        if self.RELAY_CONVEYOR_PIN:
            self.RELAY_PINS.append(self.RELAY_CONVEYOR_PIN)
            logging.info(f"[CONFIG] Đã cấu hình Relay Băng chuyền tại pin: {self.RELAY_CONVEYOR_PIN}")

        for i, lane_cfg in enumerate(lanes_config):
            new_system_lanes.append({
                "name": lane_cfg.get("name", f"Lane {i+1}"), "id": lane_cfg.get("id", f"LANE_{i+1}"),
                "status": "Sẵn sàng", "count": 0, "sensor_pin": lane_cfg.get("sensor_pin"),
                "push_pin": lane_cfg.get("push_pin"), "pull_pin": lane_cfg.get("pull_pin"),
                "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
            })
            if lane_cfg.get("sensor_pin") is not None: self.SENSOR_PINS.append(lane_cfg["sensor_pin"])
            if lane_cfg.get("push_pin") is not None: self.RELAY_PINS.append(lane_cfg["push_pin"])
            if lane_cfg.get("pull_pin") is not None: self.RELAY_PINS.append(lane_cfg["pull_pin"])

        self.last_sensor_state = [1] * num_lanes; self.last_sensor_trigger_time = [0.0] * num_lanes
        self.auto_test_last_state = [1] * num_lanes; self.auto_test_last_trigger = [0.0] * num_lanes

        with self.state_lock:
            self.system_state['timing_config'] = loaded_config['timing_config']
            self.system_state['gpio_mode'] = loaded_config['timing_config'].get("gpio_mode", "BOARD")
            self.system_state['lanes'] = new_system_lanes
            self.system_state['is_mock'] = isinstance(self.gpio, MockGPIO)
            self.system_state['sensor_entry_reading'] = 1
            self.system_state['ai_config'] = loaded_config['ai_config']
            self.system_state['camera_settings'] = loaded_config['camera_settings']
        
        # Khởi tạo AI (Lấy từ app_god.py, nhưng dùng class AIDetector)
        ai_cfg = loaded_config['ai_config']
        if ai_cfg.get('enable_ai', False):
            if not YOLO_AVAILABLE:
                logging.error("[AI] Config bật AI, nhưng 'ultralytics' chưa được cài đặt.")
            else:
                model_path = ai_cfg.get('model_path', 'yolov8n.pt')
                if not os.path.exists(model_path):
                    logging.error(f"[AI] Lỗi: Không tìm thấy file model tại '{model_path}'.")
                else:
                    self.ai_detector = AIDetector(model_path, ai_cfg)
                    if self.ai_detector.enabled:
                        # Map class name (từ AI) sang lane index (từ config)
                        lane_id_to_index_map = {canon_id(lane['id']): i for i, lane in enumerate(lanes_config) if lane.get('id')}
                        ai_class_map_config = ai_cfg.get('ai_class_to_id_map', {})
                        
                        for class_name, lane_id in ai_class_map_config.items():
                            canon_lane_id = canon_id(lane_id)
                            if canon_lane_id in lane_id_to_index_map:
                                lane_index = lane_id_to_index_map[canon_lane_id]
                                self.ai_detector.lane_map[class_name.upper()] = lane_index
                                logging.info(f"[AI] Đã map Class '{class_name.upper()}' -> Lane ID '{lane_id}' (index {lane_index})")
                            else:
                                logging.warning(f"[AI] Lỗi map: Lane ID '{lane_id}' (cho class '{class_name}') không tồn tại.")
        
        if not self.ai_detector or not self.ai_detector.enabled:
            logging.warning("[AI] Tính năng AI hiện đang TẮT (do config hoặc lỗi).")
            
        logging.info(f"[CONFIG] Loaded {num_lanes} lanes config.")
        logging.info(f"[CONFIG] Sensor Entry Pin (Real/Mock): {SENSOR_ENTRY_PIN} / {SENSOR_ENTRY_MOCK_PIN}")

    def _save_config_to_file(self, config_data):
        """Hàm trợ giúp để lưu file config"""
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=4)
            return True
        except Exception as e:
            logging.error(f"[CONFIG] Không thể tạo/lưu file config: {e}")
            return False

    def _setup_gpio(self):
        """Cấu hình chân cắm GPIO (Lấy từ app_god.py)"""
        loaded_gpio_mode = ""
        with self.state_lock:
            loaded_gpio_mode = self.system_state.get("gpio_mode", "BOARD")
        
        if isinstance(self.gpio, RealGPIO):
            mode_to_set = self.gpio.BOARD if loaded_gpio_mode == "BOARD" else self.gpio.BOARD
            self.gpio.setmode(mode_to_set)
            self.gpio.setwarnings(False)
            logging.info(f"[GPIO] Đã đặt chế độ chân cắm là: {loaded_gpio_mode}")
            
            active_sensor_pins = list(set([pin for pin in self.SENSOR_PINS if pin is not None]))
            active_relay_pins = list(set([pin for pin in self.RELAY_PINS if pin is not None]))
            
            logging.info(f"[GPIO] Setup SENSOR pins: {active_sensor_pins}")
            for pin in active_sensor_pins:
                try: self.gpio.setup(pin, self.gpio.IN, pull_up_down=self.gpio.PUD_UP)
                except Exception as e:
                    logging.critical(f"[CRITICAL] Lỗi cấu hình chân SENSOR {pin}: {e}.")
                    self.error_manager.trigger_maintenance(f"Lỗi cấu hình chân SENSOR {pin}: {e}")
                    raise
            
            logging.info(f"[GPIO] Setup RELAY pins: {active_relay_pins}")
            for pin in active_relay_pins:
                try: self.gpio.setup(pin, self.gpio.OUT)
                except Exception as e:
                    logging.critical(f"[CRITICAL] Lỗi cấu hình chân RELAY {pin}: {e}.")
                    self.error_manager.trigger_maintenance(f"Lỗi cấu hình chân RELAY {pin}: {e}")
                    raise
        else:
            logging.info("[GPIO] Chạy ở chế độ Mock, bỏ qua setup vật lý.")

    def save_queues_on_shutdown(self):
        """(Lấy từ app_god.py)"""
        logging.info("[SHUTDOWN] Đang lưu trạng thái hàng chờ...")
        try:
            queue_data = {}
            with self.qr_queue_lock:
                queue_data['qr_queue'] = list(self.qr_queue)
            with self.processing_queue_lock:
                queue_data['processing_queue'] = list(self.processing_queue)
            
            if not queue_data['qr_queue'] and not queue_data['processing_queue']:
                logging.info("[SHUTDOWN] Hàng chờ trống, không cần lưu.")
                if os.path.exists(QUEUE_STATE_FILE): os.remove(QUEUE_STATE_FILE)
                return

            with open(QUEUE_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(queue_data, f)
            logging.info(f"[SHUTDOWN] Đã lưu {len(queue_data['qr_queue'])} QR, {len(queue_data['processing_queue'])} Job.")
        except Exception as e:
            logging.error(f"[SHUTDOWN] Lỗi không thể lưu hàng chờ: {e}")

    def _load_queues_on_startup(self):
        """(Lấy từ app_god.py)"""
        if os.path.exists(QUEUE_STATE_FILE):
            logging.warning(f"[STARTUP] Phát hiện file {QUEUE_STATE_FILE}. Đang khôi phục...")
            try:
                with open(QUEUE_STATE_FILE, 'r', encoding='utf-8') as f:
                    queue_data = json.load(f)
                
                with self.qr_queue_lock:
                    self.qr_queue = queue_data.get('qr_queue', [])
                with self.processing_queue_lock:
                    self.processing_queue = queue_data.get('processing_queue', [])
                    if self.processing_queue:
                        self.queue_head_since = time.time()
                
                logging.info(f"[STARTUP] Đã khôi phục {len(self.qr_queue)} QR, {len(self.processing_queue)} Job.")
                
                with self.state_lock:
                        self.system_state["queue_indices"] = [j["lane_index"] for j in self.processing_queue]
                        self.system_state["entry_queue_size"] = len(self.processing_queue)
                        for job in self.processing_queue:
                            lane_idx = job.get('lane_index')
                            if 0 <= lane_idx < len(self.system_state['lanes']):
                                 self.system_state['lanes'][lane_idx]['status'] = "Đang chờ vật (Tải lại)"

            except Exception as e:
                logging.error(f"[STARTUP] Lỗi khôi phục hàng chờ: {e}.")
                self.qr_queue = []; self.processing_queue = []
            
            try: os.remove(QUEUE_STATE_FILE)
            except Exception as e: logging.error(f"[STARTUP] Lỗi xóa file {QUEUE_STATE_FILE}: {e}")
        else:
            logging.info("[STARTUP] Không có file trạng thái hàng chờ. Bắt đầu mới.")

    # ===========================================
    # CÁC HÀM HỖ TRỢ API & WEBSOCKET
    # ===========================================

    def add_ws_client(self, ws):
        with self.ws_lock: self.ws_clients.add(ws)
        logging.info(f"[WS] Client kết nối. Tổng: {len(self.ws_clients)}")
        
    def remove_ws_client(self, ws):
        with self.ws_lock: self.ws_clients.discard(ws)
        logging.info(f"[WS] Client ngắt kết nối. Còn lại: {len(self.ws_clients)}")

    def broadcast_log(self, log_type, message, data=None):
        """Gửi log tới tất cả client (Lấy từ app_god.py)"""
        log_data = {
            'timestamp': time.strftime('%H:%M:%S'),
            'log_type': log_type,
            'message': message,
            'data': data or {}
        }
        msg = json.dumps({"type": "log", **log_data})
        
        clients_to_send = []
        with self.ws_lock: clients_to_send = list(self.ws_clients)
        if not clients_to_send: return

        with self.broadcast_lock: 
            for client in clients_to_send:
                try: client.send(msg)
                except Exception: self.remove_ws_client(client) # Xóa client hỏng

    def get_full_state(self):
        """Lấy snapshot của state để gửi qua WS (Lấy từ app_god.py)"""
        queue_len = 0
        current_queue_indices = []
        with self.processing_queue_lock:
            queue_len = len(self.processing_queue)
            current_queue_indices = [j["lane_index"] for j in self.processing_queue]
            
        with self.state_lock:
            self.system_state["maintenance_mode"] = self.error_manager.is_maintenance()
            self.system_state["last_error"] = self.error_manager.last_error
            self.system_state["is_mock"] = isinstance(self.gpio, MockGPIO)
            # self.system_state["auth_enabled"] = AUTH_ENABLED # Sẽ do web/app.py quản lý
            self.system_state["gpio_mode"] = self.system_state['timing_config'].get('gpio_mode', 'BOARD')
            self.system_state["entry_queue_size"] = queue_len
            self.system_state["queue_indices"] = current_queue_indices
            self.system_state["sensor_entry_reading"] = self.last_entry_sensor_state 
            
            try:
                state_copy_for_json = copy.deepcopy(self.system_state)
                return state_copy_for_json
            except Exception as e:
                logging.warning(f"[BROADCAST] Lỗi khi deepcopy state: {e}")
                return None

    def get_config_for_json(self):
        """Lấy config cho API /config (Lấy từ app_god.py)"""
        with self.state_lock:
            config_data = {
                "timing_config": self.system_state.get('timing_config', {}).copy(),
                "ai_config": self.system_state.get('ai_config', {}).copy(),
                "camera_settings": self.system_state.get('camera_settings', {}).copy(),
                "lanes_config": [{
                    "id": ln.get('id'), "name": ln.get('name'),
                    "sensor_pin": ln.get('sensor_pin'), "push_pin": ln.get('push_pin'),
                    "pull_pin": ln.get('pull_pin')
                 } for ln in self.system_state.get('lanes', [])]
            }
        return config_data

    def update_config(self, new_config_data):
        """Xử lý API /update_config (Lấy từ app_god.py)"""
        if not new_config_data:
            return ({"error": "Thiếu dữ liệu JSON"}, 400)
        logging.info(f"[CONFIG] Nhận config mới từ API (POST): {new_config_data}")

        new_timing_config = new_config_data.get('timing_config', {})
        new_lanes_config = new_config_data.get('lanes_config')
        new_ai_config = new_config_data.get('ai_config')
        new_camera_settings = new_config_data.get('camera_settings')

        config_to_save = {}
        restart_required = False

        with self.state_lock:
            # Xử lý AI
            current_ai_config = self.system_state.get('ai_config', {})
            if new_ai_config is not None and new_ai_config != current_ai_config:
                logging.warning("[CONFIG] Cài đặt AI đã thay đổi. Cần khởi động lại.")
                self.broadcast_log("warn", "Cài đặt AI đã đổi. Cần khởi động lại!")
                current_ai_config.update(new_ai_config)
                self.system_state['ai_config'] = current_ai_config
                restart_required = True
            config_to_save['ai_config'] = current_ai_config.copy()
            
            # Xử lý Camera
            current_camera_settings = self.system_state.get('camera_settings', {})
            if new_camera_settings is not None and new_camera_settings != current_camera_settings:
                logging.warning("[CONFIG] Cài đặt Camera đã thay đổi. Cần khởi động lại.")
                self.broadcast_log("warn", "Cài đặt Camera đã đổi. Cần khởi động lại!")
                current_camera_settings.update(new_camera_settings)
                self.system_state['camera_settings'] = current_camera_settings
                restart_required = True
            config_to_save['camera_settings'] = current_camera_settings.copy()

            # Xử lý Timing
            current_timing = self.system_state['timing_config']
            current_gpio_mode = current_timing.get('gpio_mode', 'BOARD')
            current_use_gantry = current_timing.get('use_sensor_entry_gantry', False)
            
            # Cập nhật timing
            current_timing.update(new_timing_config)
            self.system_state['timing_config'] = current_timing
            
            # Kiểm tra thay đổi cần restart
            if current_timing.get('RELAY_CONVEYOR_PIN') != self.RELAY_CONVEYOR_PIN:
                restart_required = True; logging.warning("[CONFIG] Chân Relay Băng chuyền đổi. Cần restart.")
            if current_timing.get('gpio_mode', 'BOARD') != current_gpio_mode:
                restart_required = True; logging.warning("[CONFIG] Chế độ GPIO đổi. Cần restart.")
            if current_timing.get('use_sensor_entry_gantry', False) != current_use_gantry:
                restart_required = True; logging.warning("[CONFIG] Logic (v1/v2) đổi. Cần restart.")
            
            config_to_save['timing_config'] = current_timing.copy()

            # Xử lý Lanes
            if new_lanes_config is not None:
                lanes_config = self._ensure_lane_ids(new_lanes_config)
                config_to_save['lanes_config'] = lanes_config
                restart_required = True
                logging.warning("[CONFIG] Cấu hình lanes đã thay đổi. Cần khởi động lại ứng dụng.")
                self.broadcast_log("warn", "Cấu hình Lanes đã đổi. Cần khởi động lại!")
            else:
                config_to_save['lanes_config'] = [
                    {"id": l.get('id'), "name": l['name'], "sensor_pin": l.get('sensor_pin'),
                     "push_pin": l.get('push_pin'), "pull_pin": l.get('pull_pin')}
                    for l in self.system_state['lanes']
                ]

        try:
            with self.config_file_lock:
                self._save_config_to_file(config_to_save)
            
            msg = "Đã lưu config. "
            if restart_required: msg += "Vui lòng khởi động lại hệ thống để áp dụng thay đổi."
            else: msg += "Các thay đổi về timing đã được áp dụng."
            logging.info(f"[CONFIG] {msg}")
            self.broadcast_log("info", msg)
            
            return ({"message": msg, "config": config_to_save, "restart_required": restart_required}, 200)

        except Exception as e:
            logging.error(f"[ERROR] Không thể lưu config (POST): {e}")
            self.broadcast_log("error", f"Lỗi khi lưu config (POST): {e}")
            return ({"error": str(e)}, 500)

    def handle_ws_message(self, data, client_label="guest"):
        """Xử lý tin nhắn đến từ WS (Lấy từ app_god.py)"""
        action = data.get('action')
        if self.error_manager.is_maintenance() and action != "reset_maintenance":
            self.broadcast_log("error", "Hệ thống đang bảo trì, không thể thao tác.")
            return

        if action == 'reset_count':
            lane_idx_str = data.get('lane_index')
            with self.state_lock:
                if lane_idx_str == 'all':
                    for i in range(len(self.system_state['lanes'])): self.system_state['lanes'][i]['count'] = 0
                    self.broadcast_log("info", f"{client_label} đã reset đếm toàn bộ.")
                else:
                    try:
                        lane_idx = int(lane_idx_str)
                        if 0 <= lane_idx < len(self.system_state['lanes']):
                            lane_name = self.system_state['lanes'][lane_idx]['name']
                            self.system_state['lanes'][lane_idx]['count'] = 0
                            self.broadcast_log("info", f"{client_label} đã reset đếm {lane_name}.")
                    except (ValueError, TypeError):
                        logging.warning(f"[WS] Invalid lane_index_str: {lane_idx_str}")


        elif action == "test_relay":
            lane_index = data.get("lane_index"); relay_action = data.get("relay_action")
            if lane_index is not None and relay_action:
                self.executor.submit(test_utils.run_test_relay, self, lane_index, relay_action)
        
        elif action == "test_all_relays":
            self.executor.submit(test_utils.run_test_all_relays, self)
            
        elif action == "toggle_auto_test":
            self.auto_test_enabled = data.get("enabled", False)
            logging.info(f"[TEST] Auto-Test (Sensor->Relay) set by {client_label} to: {self.auto_test_enabled}")
            self.broadcast_log("warn", f"Chế độ Auto-Test đã { 'BẬT' if self.auto_test_enabled else 'TẮT' } bởi {client_label}.")
            if not self.auto_test_enabled:
                self.reset_all_relays_to_default()
        
        elif action == "reset_maintenance":
            self.reset_maintenance_mode()

    def reset_maintenance_mode(self):
        """Reset bảo trì (Lấy từ app_god.py)"""
        if self.error_manager.is_maintenance():
            self.error_manager.reset()
            with self.qr_queue_lock:
                self.qr_queue.clear()
            with self.processing_queue_lock:
                self.processing_queue.clear()
                self.queue_head_since = 0.0
            
            self.last_entry_sensor_state = 1
            self.last_entry_sensor_trigger_time = 0.0

            with self.state_lock:
                self.system_state["queue_indices"] = []
                self.system_state["entry_queue_size"] = 0
                self.system_state["sensor_entry_reading"] = 1
                
            self.broadcast_log("success", "Chế độ bảo trì đã được reset. Hàng chờ đã được xóa.")
            return ({"message": "Maintenance mode reset thành công."}, 200)
        else:
            return ({"message": "Hệ thống không ở chế độ bảo trì."}, 200)

    def reset_queues(self):
        """Reset hàng chờ (Lấy từ app_god.py)"""
        if self.error_manager.is_maintenance():
            return ({"error": "Hệ thống đang bảo trì, không thể reset hàng chờ."}, 403)
        try:
            with self.qr_queue_lock:
                self.qr_queue.clear()
            with self.processing_queue_lock:
                self.processing_queue.clear()
                self.queue_head_since = 0.0
                current_queue_for_log = []

            with self.state_lock:
                for lane in self.system_state["lanes"]:
                    lane["status"] = "Sẵn sàng"
                self.system_state["queue_indices"] = current_queue_for_log
                self.system_state["entry_queue_size"] = 0
                
            self.broadcast_log("warn", "Tất cả hàng chờ (Tạm & Chính) đã được reset thủ công.", data={"queue": current_queue_for_log})
            logging.info("[API] Tất cả hàng chờ đã được reset thủ công.")
            return ({"message": "Hàng chờ đã được reset."}, 200)
        except Exception as e:
            logging.error(f"[API] Lỗi khi reset hàng chờ: {e}")
            return ({"error": str(e)}, 500)

    def mock_gpio_sensor(self, payload):
        """Giả lập sensor (Lấy từ app_god.py)"""
        if not isinstance(self.gpio, MockGPIO):
            return ({"error": "Chức năng chỉ khả dụng ở chế độ mô phỏng."}, 400)
        
        lane_index = payload.get('lane_index')
        pin = payload.get('pin'); requested_state = payload.get('state')
        pin_to_mock = None; lane_name = "N/A"

        if lane_index is not None and pin is None:
            try: lane_index = int(lane_index)
            except (TypeError, ValueError): return ({"error": "lane_index không hợp lệ."}, 400)
            with self.state_lock:
                if 0 <= lane_index < len(self.system_state['lanes']):
                    pin_to_mock = self.system_state['lanes'][lane_index].get('sensor_pin')
                    lane_name = self.system_state['lanes'][lane_index].get('name', lane_name)
                else: return ({"error": "lane_index vượt ngoài phạm vi."}, 400)
        
        elif pin is not None:
            try: pin_to_mock = int(pin)
            except (TypeError, ValueError): return ({"error": "Giá trị pin không hợp lệ."}, 400)
            if pin_to_mock == SENSOR_ENTRY_PIN:
                pin_to_mock = SENSOR_ENTRY_MOCK_PIN; lane_name = "SENSOR_ENTRY (Real Pin)"
            elif pin_to_mock == SENSOR_ENTRY_MOCK_PIN: lane_name = "SENSOR_ENTRY (Mock Pin)"
            else:
                with self.state_lock:
                     for lane in self.system_state['lanes']:
                        if lane.get('sensor_pin') == pin_to_mock:
                            lane_name = lane.get('name', f"Pin {pin_to_mock}"); break

        if pin_to_mock is None: return ({"error": "Thiếu thông tin chân sensor."}, 400)
        
        # Ép kiểu self.gpio thành MockGPIO để gọi hàm set_input_state
        mock_gpio_instance = self.gpio
        
        if requested_state is None: logical_state = mock_gpio_instance.toggle_input_state(pin_to_mock)
        else:
            logical_state = 1 if str(requested_state).strip().lower() in {"1", "true", "high", "inactive"} else 0
            mock_gpio_instance.set_input_state(pin_to_mock, logical_state)

        with self.state_lock:
            if pin_to_mock == SENSOR_ENTRY_MOCK_PIN:
                self.system_state['sensor_entry_reading'] = 0 if logical_state == 0 else 1
                self.last_entry_sensor_state = self.system_state['sensor_entry_reading']
            else:
                for lane in self.system_state['lanes']:
                    if lane.get('sensor_pin') == pin_to_mock:
                        lane['sensor_reading'] = 0 if logical_state == 0 else 1; break
                        
        state_label = 'ACTIVE (LOW)' if logical_state == 0 else 'INACTIVE (HIGH)'
        message = f"[MOCK] Sensor pin {pin_to_mock} -> {state_label} ({lane_name})";
        self.broadcast_log("info", message)
        return ({"pin": pin_to_mock, "state": logical_state, "lane": lane_name}, 200)