import cv2
import time
import json
import threading
import os
import unicodedata
import re
import RPi.GPIO as GPIO # Import trực tiếp
from flask import Flask, Response, send_from_directory, request, jsonify
from flask_sock import Sock
try:
    from pyzbar import pyzbar
    PYZBAR_ENABLED = True
except ImportError:
    PYZBAR_ENABLED = False
    print("[WARN] Thư viện pyzbar chưa được cài đặt (pip install pyzbar). Sẽ chỉ dùng cv2.QRCodeDetector().")


# =============================
#      CẤU HÌNH & KHỞI TẠO TOÀN CỤC
# =============================
CONFIG_FILE = 'config.json'
ACTIVE_LOW = True

# --- Các biến toàn cục ---
lanes_config = []       # Tải từ JSON
timing_config = {}      # Tải từ JSON
qr_config = {}          # Tải từ JSON

RELAY_PINS = []
SENSOR_PINS = []

main_running = True
latest_frame = None
frame_lock = threading.Lock()

ws_clients, ws_lock = set(), threading.Lock()


counts = []             # Bộ đếm (khởi tạo theo num_lanes)
last_s_state = []       # Trạng thái sensor (khởi tạo theo num_lanes)
last_s_trig = []        # Thời điểm trigger (khởi tạo theo num_lanes)
qr_queue = []           # Hàng chờ (lưu index)


relay_grab_state = []   # Trạng thái relay Thu (1=ON, 0=OFF)
relay_push_state = []   # Trạng thái relay Đẩy (1=ON, 0=OFF)


queue_lock = threading.Lock()
queue_head_since = 0.0

# =============================
#    CÁC HÀM TIỆN ÍCH (Chuẩn hóa ID)
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
#       HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    if pin is None: return
    try: GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
    except Exception as e:
        log(f"[GPIO] Lỗi kích hoạt relay pin {pin}: {e}", 'error')
        global main_running
        main_running = False

def RELAY_OFF(pin):
    if pin is None: return
    try: GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)
    except Exception as e:
        log(f"[GPIO] Lỗi tắt relay pin {pin}: {e}", 'error')
        global main_running
        main_running = False

# =============================
#      LOAD CẤU HÌNH
# =============================
def ensure_lane_ids(lanes_list):
    default_ids = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    for i, lane in enumerate(lanes_list):
        if 'id' not in lane or not lane['id']:
            lane['id'] = default_ids[i] if i < len(default_ids) else f"LANE_{i+1}"
    return lanes_list

def load_config():
    global lanes_config, timing_config, qr_config, RELAY_PINS, SENSOR_PINS
    # Thêm các biến relay vào global
    global counts, last_s_state, last_s_trig, relay_grab_state, relay_push_state

    if not os.path.exists(CONFIG_FILE):
        print(f"[CRITICAL] Không tìm thấy file {CONFIG_FILE}. Không thể khởi động.")
        return False

    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f: content = f.read()
        file_cfg = json.loads(content)
        
        timing_config = file_cfg.get('timing_config', {})
        qr_config = file_cfg.get('qr_config', {})
        lanes_config = ensure_lane_ids(file_cfg.get('lanes_config', []))
        
        num_lanes = len(lanes_config)
        
        RELAY_PINS.clear(); SENSOR_PINS.clear()
        for i, cfg in enumerate(lanes_config):
            s_pin = int(cfg["sensor_pin"]) if cfg.get("sensor_pin") is not None else None
            p_pin = int(cfg["push_pin"]) if cfg.get("push_pin") is not None else None
            pl_pin = int(cfg["pull_pin"]) if cfg.get("pull_pin") is not None else None
            
            cfg["index"] = i # Thêm index để tham chiếu
            if s_pin is not None: SENSOR_PINS.append(s_pin)
            if p_pin is not None: RELAY_PINS.append(p_pin)
            if pl_pin is not None: RELAY_PINS.append(pl_pin)

        # Khởi tạo các mảng trạng thái
        counts = [0] * num_lanes
        last_s_state = [1] * num_lanes
        last_s_trig = [0.0] * num_lanes
        # (ĐÃ XÓA) pending_sensor_triggers
        
        # Giả định mặc định là Thu BẬT (1), Đẩy TẮT (0)
        relay_grab_state = [1] * num_lanes 
        relay_push_state = [0] * num_lanes
        # Gán lại cho các lane không có relay
        for i, lane in enumerate(lanes_config):
             if lane.get("pull_pin") is None:
                 relay_grab_state[i] = 0
        
        print(f"[CONFIG] Đã tải cấu hình cho {num_lanes} lanes.")
        return True
    except Exception as e:
        print(f"[CRITICAL] Lỗi đọc file {CONFIG_FILE}: {e}. Không thể khởi động.")
        return False

def reset_relays():
    global relay_grab_state, relay_push_state
    print("[GPIO] Reset tất cả relay (Thu BẬT, Đẩy TẮT)...")
    try:
        for i, lane in enumerate(lanes_config):
            pull_pin, push_pin = lane.get("pull_pin"), lane.get("push_pin")
            
            if pull_pin is not None:
                RELAY_ON(pull_pin)
                relay_grab_state[i] = 1 
            else:
                relay_grab_state[i] = 0 
                
            if push_pin is not None:
                RELAY_OFF(push_pin)
                relay_push_state[i] = 0
            else:
                relay_push_state[i] = 0 

        time.sleep(0.1)
        print("[GPIO] Reset relay hoàn tất.")
    except Exception as e:
        log(f"[GPIO] Lỗi khi reset relay: {e}", 'error')
        global main_running
        main_running = False

# =============================
# HÀM HỖ TRỢ (Log & Broadcast)
# =============================
def log(msg, log_type="info"):
    """In ra console và gửi log tới client."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")
    broadcast({"type": "log", "log_type": log_type, "message": msg})

def broadcast(event):
    if event.get("type") == "log":
        event['timestamp'] = time.strftime('%H:%M:%S')
    data = json.dumps(event)
    with ws_lock:
        for ws in list(ws_clients):
            try: ws.send(data)
            except: ws_clients.remove(ws)

# =============================
#         LUỒNG CAMERA
# =============================
def run_camera(camera_index):
    global latest_frame, main_running
    camera = None
    try:
        print("[CAMERA] Khởi tạo camera...")
        camera = cv2.VideoCapture(camera_index)
        props = {cv2.CAP_PROP_FRAME_WIDTH: 640, cv2.CAP_PROP_FRAME_HEIGHT: 480, cv2.CAP_PROP_BUFFERSIZE: 1}
        for prop, value in props.items(): camera.set(prop, value)

        if not camera.isOpened():
            log("[CRITICAL] Không thể mở camera. Dừng hệ thống.", 'error')
            main_running = False
            return
        print("[CAMERA] Camera sẵn sàng.")
        
        while main_running:
            ret, frame = camera.read()
            if not ret:
                log("[CRITICAL] Mất kết nối camera. Dừng hệ thống.", 'error')
                main_running = False
                break
            with frame_lock:
                latest_frame = frame.copy()
            time.sleep(1 / 60)
    except Exception as e:
        log(f"[CRITICAL] Luồng camera bị crash: {e}", 'error')
        main_running = False
    finally:
        if camera: camera.release()
        print("[CAMERA] Đã giải phóng camera.")

# =============================
#       LOGIC CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index):
    global counts, relay_grab_state, relay_push_state
    try:
        lane = lanes_config[lane_index]
        lane_name = lane["name"]
        push_pin = lane.get("push_pin")
        pull_pin = lane.get("pull_pin")
        
        cfg = timing_config
        delay = cfg.get('cycle_delay', 0.3)
        settle_delay = cfg.get('settle_delay', 0.2)
        
        is_sorting_lane = not (push_pin is None or pull_pin is None)

        if not is_sorting_lane:
            log(f"Vật phẩm đi thẳng qua {lane_name}", 'pass')
            log_type = "pass"
        else:
            log(f"Bắt đầu chu trình đẩy {lane_name}", 'info')
            
            RELAY_OFF(pull_pin)
            relay_grab_state[lane_index] = 0 
            time.sleep(settle_delay)
            if not main_running: return
            
            RELAY_ON(push_pin)
            relay_push_state[lane_index] = 1 
            time.sleep(delay)
            if not main_running: return
            
            RELAY_OFF(push_pin)
            relay_push_state[lane_index] = 0 
            time.sleep(settle_delay)
            if not main_running: return
            
            RELAY_ON(pull_pin)
            relay_grab_state[lane_index] = 1 
            
            log_type = "sort"
        
        counts[lane_index] += 1
        log(f"Hoàn tất: {lane_name}. Tổng đếm: {counts[lane_index]}", log_type)
        # Gửi log đếm cho UI
        broadcast({"type": "log", "log_type": log_type, "name": lane_name, "count": counts[lane_index]})

    except Exception as e:
        log(f"[SORT] Lỗi trong sorting_process (lane {lane_name}): {e}", 'error')
        global main_running
        main_running = False

def handle_sorting_with_delay(lane_index):
    try:
        lane_name_for_log = lanes_config[lane_index]['name']
        push_delay = timing_config.get('push_delay', 0.2)

        if push_delay > 0:
            log(f"Đã thấy vật {lane_name_for_log}, chờ {push_delay}s...", 'info')
            time.sleep(push_delay)
        if not main_running: return
        
        sorting_process(lane_index)

    except Exception as e:
        log(f"[ERROR] Lỗi trong luồng sorting_delay (lane {lane_name_for_log}): {e}", 'error')
        global main_running
        main_running = False

# =============================
#       QUÉT MÃ QR 
# =============================
def qr_detection_loop():
    global queue_head_since
    
    detector = cv2.QRCodeDetector() # Vẫn giữ detector của CV2
    last_qr, last_time = "", 0.0
    
    # Log khởi động
    if PYZBAR_ENABLED:
        print("[QR] Luồng QR bắt đầu (Ưu tiên: Pyzbar, Dự phòng: CV2).")
    else:
        print("[QR] Luồng QR bắt đầu (Sử dụng: cv2.QRCodeDetector).")
    
    # (ĐÃ XÓA) PENDING_TRIGGER_TIMEOUT

    while main_running:
        try:
            LANE_MAP = {canon_id(l.get("id")): l["index"] for l in lanes_config if l.get("id")}
            
            cfg = qr_config
            use_roi = cfg.get("use_roi", False)
            x, y = cfg.get("roi_x", 0), cfg.get("roi_y", 0)
            w, h = cfg.get("roi_w", 0), cfg.get("roi_h", 0)

            frame_copy = None
            with frame_lock:
                if latest_frame is not None: frame_copy = latest_frame.copy()
            if frame_copy is None:
                time.sleep(0.01); continue

            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if use_roi and w > 0 and h > 0:
                y_end = min(y + h, gray_frame.shape[0])
                x_end = min(x + w, gray_frame.shape[1])
                gray_frame = gray_frame[y:y_end, x:x_end]

            # --- LOGIC QUÉT PYZBAR + CV2 ---
            data = None
            qr_source = None

            if PYZBAR_ENABLED:
                try:
                    decoded = pyzbar.decode(gray_frame)
                    if decoded:
                        raw = decoded[0].data
                        data = raw.decode('utf-8', errors='ignore').strip('\x00')
                        qr_source = "Pyzbar"
                except Exception:
                    data = None # Lỗi decode pyzbar

            if not data:
                try:
                    data_cv2, _, _ = detector.detectAndDecode(gray_frame)
                    if data_cv2:
                        data = data_cv2
                        qr_source = "CV2"
                except Exception:
                    data = None # Lỗi cv2

            if data and (data != last_qr or time.time() - last_time > 3.0):
                last_qr, last_time = data, time.time()
                data_key = canon_id(data)
                data_raw = data.strip()
                now = time.time()
                
                qr_source_log = f"({qr_source}) " if qr_source else ""

                # --- (CẬP NHẬT) LOGIC FIFO NGHIÊM NGẶT (Strict FIFO) ---
                if data_key in LANE_MAP:
                    idx = LANE_MAP[data_key]
                    
                    # Luôn thêm vào hàng chờ. Không check sensor-first.
                    with queue_lock:
                        is_queue_empty_before = not qr_queue
                        qr_queue.append(idx)
                        current_queue_for_log = list(qr_queue) # Gửi index cho UI
                        if is_queue_empty_before: queue_head_since = time.time()
                    
                    msg = f"Phát hiện {lanes_config[idx]['name']} (key: {data_key}). Đã thêm vào hàng chờ."
                    log(f"[QR] {qr_source_log}{msg}", 'qr')
                    broadcast({"type": "log", "log_type": "qr", "message": msg, "data": {"queue": current_queue_for_log}})
                # --- (HẾT CẬP NHẬT) ---
                            
                elif data_key == "NG":
                    log(f"[QR] {qr_source_log}Mã NG: {data_raw}", 'warn')
                else:
                    log(f"[QR] {qr_source_log}Không rõ mã QR: raw='{data_raw}', key='{data_key}'", 'warn')
                    broadcast({"type": "log", "log_type": "unknown_qr", "message": f"Không rõ mã QR: {data_raw}"})
            
            time.sleep(0.01)

        except Exception as e:
            log(f"[QR] Lỗi trong luồng QR: {e}", 'error')
            time.sleep(0.5)

# =============================
#      GIÁM SÁT SENSOR 
# =============================
def sensor_monitoring_thread():
    global last_s_state, last_s_trig, queue_head_since
    
    debounce_time = timing_config.get('sensor_debounce', 0.1)
    QUEUE_HEAD_TIMEOUT = timing_config.get('queue_head_timeout', 15.0)
    num_lanes = len(lanes_config)
    
    # Biến theo dõi trạng thái trước đó để phát hiện sườn xuống
    last_s_state_prev = list(last_s_state) 

    try:
        while main_running:
            now = time.time()

            with queue_lock:
                if qr_queue and (now - queue_head_since) > QUEUE_HEAD_TIMEOUT:
                    expected_lane_index = qr_queue.pop(0)
                    expected_lane_name = lanes_config[expected_lane_index]['name']
                    current_queue_for_log = list(qr_queue)
                    queue_head_since = now if qr_queue else 0.0
                    
                    msg = f"TIMEOUT! Tự động xóa {expected_lane_name} khỏi hàng chờ."
                    log(f"[SENSOR] {msg}", 'warn')
                    broadcast({"type": "log", "log_type": "warn", "message": msg, "data": {"queue": current_queue_for_log}})

            for i in range(num_lanes):
                lane = lanes_config[i]
                sensor_pin = lane.get("sensor_pin")
                if sensor_pin is None: continue
                
                lane_name = lane['name']
                push_pin = lane.get("push_pin")

                try:
                    sensor_now = GPIO.input(sensor_pin)
                    # (SỬA) Chỉ cập nhật trạng thái này cho UI
                    last_s_state[i] = sensor_now 
                except Exception as gpio_e:
                    log(f"[SENSOR] Lỗi đọc GPIO pin {sensor_pin} ({lane_name}): {gpio_e}", 'error')
                    global main_running
                    main_running = False
                    break
                
                #Phát hiện sườn xuống (1 -> 0)
                if sensor_now == 0 and last_s_state_prev[i] == 1:
                     
                     # Logic debounce
                     if (now - last_s_trig[i]) > debounce_time:
                        last_s_trig[i] = now # Ghi lại thời điểm trigger

                        # --- (CẬP NHẬT) LOGIC FIFO NGHIÊM NGẶT (Strict FIFO) ---
                        with queue_lock:
                            current_queue_for_log = list(qr_queue)

                            if not qr_queue:
                                # --- 1. HÀNG CHỜ RỖNG ---
                                # Nếu là lane đi thẳng (không cần QR), cho chạy luôn
                                if push_pin is None:
                                    log(f"Vật đi thẳng (không QR) qua {lane_name}.", 'info')
                                    threading.Thread(target=sorting_process, args=(i,), daemon=True).start()
                                else:
                                    # Nếu là lane cần QR, nhưng queue rỗng -> Lỗi
                                    log(f"Sensor {lane_name} kích hoạt, nhưng hàng chờ rỗng. Bỏ qua.", 'warn')
                            
                            elif qr_queue[0] == i:
                                # --- 2. KHỚP ĐẦU HÀNG CHỜ ---
                                qr_queue.pop(0) # Xóa job khỏi đầu hàng chờ
                                current_queue_for_log = list(qr_queue)
                                queue_head_since = now if qr_queue else 0.0 # Reset timeout

                                threading.Thread(target=handle_sorting_with_delay, args=(i,), daemon=True).start()
                                log(f"Sensor {lane_name} khớp (Strict FIFO).", 'info')
                                broadcast({"type": "log", "log_type": "info", "message": f"Sensor {lane_name} khớp.", "data": {"queue": current_queue_for_log}})
                            
                            else:
                                # --- 3. KHÔNG KHỚP (Lỗi đồng bộ) ---
                                expected_lane_index = qr_queue[0]
                                expected_lane_name = "Không rõ"
                                try:
                                    expected_lane_name = lanes_config[expected_lane_index]['name']
                                except Exception: pass
                                
                                log(f"Sensor {lane_name} (lane {i}) kích hoạt, nhưng KHÔNG KHỚP với đầu hàng chờ ({expected_lane_name} - lane {expected_lane_index}). Bỏ qua.", 'warn')
                                broadcast({"type": "log", "log_type": "warn", "message": f"Lỗi đồng bộ: Sensor {lane_name} kích hoạt. Chờ: {expected_lane_name}.", "data": {"queue": current_queue_for_log}})
                        # --- (HẾT CẬP NHẬT) ---
                
                # (SỬA) Cập nhật trạng thái trước đó
                last_s_state_prev[i] = sensor_now

            adaptive_sleep = 0.05 if all(s == 1 for s in last_s_state) else 0.01
            time.sleep(adaptive_sleep)

    except Exception as e:
        log(f"[CRITICAL] Luồng sensor bị crash: {e}", 'error')
        main_running = False

# =============================
# LUỒNG GỬI STATE CHO UI
# =============================
def broadcast_state_thread():
    """Luồng riêng để gửi trạng thái (sensor, count, queue) cho UI."""
    global last_s_state, counts, qr_queue, relay_grab_state, relay_push_state
    while main_running:
        try:
            # Tạo snapshot trạng thái
            lanes_snapshot = []
            
            # Đảm bảo các mảng đồng bộ kích thước
            num_lanes_cfg = len(lanes_config)
            if len(last_s_state) != num_lanes_cfg or len(counts) != num_lanes_cfg or \
               len(relay_grab_state) != num_lanes_cfg or len(relay_push_state) != num_lanes_cfg:
                
                print("[WARN] Phát hiện thay đổi config, chờ đồng bộ...")
                time.sleep(1.0) # Đợi load_config chạy xong
                continue

            for i, lane_cfg in enumerate(lanes_config):
                lanes_snapshot.append({
                    "name": lane_cfg['name'],
                    "count": counts[i],
                    "sensor_reading": last_s_state[i],
                    
                    #  Gửi trạng thái relay thật ---
                    "relay_grab": relay_grab_state[i],
                    "relay_push": relay_push_state[i],
                  
                    "status": "Sẵn sàng" 
                })
            
            with queue_lock:
                queue_snapshot = list(qr_queue)

            state_data = {
                "type": "state_update",
                "state": {
                    "lanes": lanes_snapshot,
                    "queue_indices": queue_snapshot # Gửi queue index cho UI
                }
            }
            broadcast(state_data)
        except Exception as e:
            print(f"[ERROR] Lỗi broadcast state: {e}")
        
        time.sleep(0.5) # Gửi state 2 lần/giây

# =============================
#  FLASK + WEBSOCKET
# =============================
app = Flask(__name__, static_folder=".")
sock = Sock(app)

@app.route("/")
def index():
    return send_from_directory(os.path.dirname(__file__), "index_lite.html")

@app.route("/video_feed")
def video_feed():
    def gen():
        while main_running:
            frame = None
            with frame_lock:
                if latest_frame is not None:
                    frame = latest_frame.copy()
            if frame is None:
                # Tạo frame đen nếu không có camera
                import numpy as np
                frame = np.zeros((480, 640, 3), dtype=np.uint8)
                cv2.putText(frame, "No Signal", (220, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (128, 128, 128), 2)
                
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if not ok: continue
            yield b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + buf.tobytes() + b"\r\n"
            time.sleep(1/20) # Stream 20 FPS
    return Response(gen(), mimetype="multipart/x-mixed-replace; boundary=frame")

@app.route("/config")
def get_config():
    return jsonify({
        "timing_config": timing_config,
        "lanes_config": lanes_config,
        "qr_config": qr_config
    })

@sock.route("/ws")
def ws(ws):
    with ws_lock: ws_clients.add(ws)
    print(f"[WS] Client kết nối. Tổng: {len(ws_clients)}")
    try:
        while True:
            msg = ws.receive()
            if not msg: break
            data = json.loads(msg)
            act = data.get("action")
            
            if act == "reset_count":
                global counts
                counts = [0] * len(lanes_config)
                log("Đã reset toàn bộ bộ đếm.", 'warn')
            
            elif act == "reset_queue":
                with queue_lock:
                    global queue_head_since
                    qr_queue.clear()
                    queue_head_since = 0.0
                    # (ĐÃ XÓA) pending_sensor_triggers
                log("Reset hàng chờ.", 'warn')
                broadcast({"type": "log", "log_type": "warn", "message": "Hàng chờ đã được reset.", "data": {"queue": []}})
    finally:
        with ws_lock: ws_clients.discard(ws)
        print(f"[WS] Client ngắt kết nối. Còn lại: {len(ws_clients)}")

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    try:
        print("--- HỆ THỐNG ĐANG KHỞI ĐỘNG ---")

        if not load_config():
            raise RuntimeError("Không thể tải file config.json.")
        
        GPIO.setmode(GPIO.BOARD if timing_config.get("gpio_mode", "BOARD") == "BOARD" else GPIO.BCM)
        GPIO.setwarnings(False)
        print(f"[GPIO] Cài đặt chân SENSOR: {SENSOR_PINS}")
        for pin in SENSOR_PINS: GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        print(f"[GPIO] Cài đặt chân RELAY: {RELAY_PINS}")
        for pin in RELAY_PINS: GPIO.setup(pin, GPIO.OUT)
        
        reset_relays()
        
        CAM_IDX = qr_config.get("camera_index", 0)

        # Khởi động các luồng
        threading.Thread(target=run_camera, args=(CAM_IDX,), name="CameraThread", daemon=True).start()
        threading.Thread(target=qr_detection_loop, name="QRScannerThread", daemon=True).start()
        threading.Thread(target=sensor_monitoring_thread, name="SensorMonThread", daemon=True).start()
        threading.Thread(target=broadcast_state_thread, name="StateBcastThread", daemon=True).start()

        time.sleep(1)
        if not main_running:
             raise RuntimeError("Khởi động luồng thất bại (Camera hoặc GPIO).")

        print("="*55 + f"\n HỆ THỐNG PHÂN LOẠI SẴN SÀNG \n" +
                     f" Logic: FIFO Nghiêm ngặt (Đã cập nhật)\n" +
                     f" Truy cập: http://<IP_CUA_PI>:3000\n" + "="*55)
        
        app.run(host="0.0.0.0", port=3000)

    except KeyboardInterrupt:
        print("\n--- HỆ THỐNG ĐANG TẮT (Ctrl+C) ---")
    except Exception as startup_err:
        print(f"[CRITICAL] Khởi động hệ thống thất bại: {startup_err}")
    finally:
        main_running = False
        time.sleep(0.5)
        try:
            GPIO.cleanup()
            print("Dọn dẹp GPIO thành công.")
        except Exception as cleanup_err:
            print(f"Lỗi khi dọn dẹp GPIO: {cleanup_err}")
        print("--- HỆ THỐNG ĐÃ TẮT HOÀN TOÀN ---")