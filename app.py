# pi/app.py
import cv2
import time
import json
import logging
import threading
import os
import functools

from flask import Flask, render_template, Response, jsonify, request
from flask_sock import Sock

# Import hệ thống cốt lõi
from core.system import SortingSystem, LOG_FILE, DATABASE_FILE, QUEUE_STATE_FILE, CONFIG_FILE, SENSOR_ENTRY_MOCK_PIN

# ==================================================
# THIẾT LẬP LOGGING (Lấy từ app_god.py)
# ==================================================
from logging.handlers import RotatingFileHandler
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
logging.basicConfig(
    handlers=[handler, logging.StreamHandler()],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.info("[SYSTEM] Log system initialized ✅")
logging.info(f"[PATH] CONFIG_FILE: {CONFIG_FILE}")
logging.info(f"[PATH] LOG_FILE: {LOG_FILE}")
logging.info(f"[PATH] DATABASE_FILE: {DATABASE_FILE}")
logging.info(f"[PATH] QUEUE_STATE_FILE: {QUEUE_STATE_FILE}")

# ==================================================
# KHỞI TẠO FLASK VÀ HỆ THỐNG
# ==================================================
app = Flask(__name__, template_folder=os.path.join(os.path.dirname(BASE_DIR), 'templates'))
sock = Sock(app)

# Tạo một (và chỉ một) instance của SortingSystem
# Instance này sẽ quản lý tất cả state, threads, và logic.
system = SortingSystem()

# ==================================================
# LOGIC XÁC THỰC (Lấy từ app_god.py)
# ==================================================
AUTH_ENABLED = os.environ.get("APP_AUTH_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
USERNAME = os.environ.get("APP_USERNAME", "admin")
PASSWORD = os.environ.get("APP_PASSWORD", "123")
system.system_state["auth_enabled"] = AUTH_ENABLED # Cập nhật state

def check_auth(username, password):
    if not AUTH_ENABLED: return True
    return username == USERNAME and password == PASSWORD

def authenticate():
    return Response('Yêu cầu đăng nhập.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_ENABLED: return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

# ==================================================
# CÁC ROUTE CỦA FLASK (Lấy từ app_god.py)
# ==================================================

@app.route('/')
@requires_auth
def index():
    # Sử dụng template index_v1.html (bản đầy đủ)
    return render_template('index_v1.html') 

@app.route('/video_feed')
@requires_auth
def video_feed():
    def generate_frames():
        while system.main_loop_running:
            frame = None
            if not system.error_manager.is_maintenance():
                with system.frame_lock:
                    if system.latest_frame is not None:
                        frame = system.latest_frame.copy()
            
            if frame is None:
                import numpy as np
                frame = np.zeros((480, 640, 3), dtype=np.uint8)
                msg = "MAINTENANCE MODE" if system.error_manager.is_maintenance() else "NO SIGNAL"
                cv2.putText(frame, msg, (150, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
                time.sleep(0.1)
            
            try:
                fps_text = f"FPS: {system.fps_value:.2f}"
                color = (0, 255, 255) if system.error_manager.is_maintenance() else (0, 128, 0)
                cv2.putText(frame, fps_text, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2, cv2.LINE_AA)
            except Exception: pass # Bỏ qua nếu lỗi vẽ

            try:
                _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
                yield (b'--frame\r\n'
                        b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
            except Exception as encode_e:
                logging.error(f"[CAMERA] Lỗi encode frame: {encode_e}")
                
            time.sleep(1 / 30) # Stream 30 FPS
            
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/config')
@requires_auth
def get_config():
    return jsonify(system.get_config_for_json())

@app.route('/api/sort_log')
@requires_auth
def get_sort_log():
    data = system.get_sort_log_data()
    if "error" in data:
        return jsonify(data), 500
    return jsonify(data)

@app.route('/update_config', methods=['POST'])
@requires_auth
def update_config():
    response_data, status_code = system.update_config(request.json)
    return jsonify(response_data), status_code

@app.route('/api/reset_maintenance', methods=['POST'])
@requires_auth
def reset_maintenance():
    response_data, status_code = system.reset_maintenance_mode()
    return jsonify(response_data), status_code

@app.route('/api/queue/reset', methods=['POST'])
@requires_auth
def api_queue_reset():
    response_data, status_code = system.reset_queues()
    return jsonify(response_data), status_code

@app.route('/api/mock_gpio', methods=['POST'])
@requires_auth
def api_mock_gpio():
    payload = request.get_json(silent=True) or {}
    response_data, status_code = system.mock_gpio_sensor(payload)
    return jsonify(response_data), status_code

# ==================================================
# ROUTE WEBSOCKET (Lấy từ app_god.py)
# ==================================================
@sock.route('/ws')
@requires_auth
def ws_route(ws):
    auth_user = "guest";
    if AUTH_ENABLED:
        auth = request.authorization
        auth_user = auth.username
    client_label = f"{auth_user}-{id(ws):x}"
    
    system.add_ws_client(ws)
    
    try:
        # Gửi state ban đầu
        initial_state = system.get_full_state()
        if initial_state:
            initial_state["auth_enabled"] = AUTH_ENABLED # Thêm trạng thái auth
            ws.send(json.dumps({"type": "state_update", "state": initial_state}))
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}")
        system.remove_ws_client(ws); return

    try:
        while True:
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    system.handle_ws_message(data, client_label)
                except json.JSONDecodeError: pass
                except Exception as ws_loop_e: logging.error(f"[WS] Lỗi xử lý message: {ws_loop_e}")
    except Exception as ws_conn_e:
        logging.warning(f"[WS] Kết nối WebSocket bị đóng hoặc lỗi: {ws_conn_e}")
    finally:
        system.remove_ws_client(ws)

# ==================================================
# KHỞI CHẠY HỆ THỐNG
# ==================================================
if __name__ == "__main__":
    try:
        # 1. Khởi động TẤT CẢ các luồng nền của SortingSystem (camera, sensors, logic, v.v.)
        #    Chúng sẽ chạy trong background (daemon=True)
        threading.Thread(target=system.run, name="SystemMainLoop", daemon=True).start()

        logging.info("=========================================")
        logging.info("    HỆ THỐNG PHÂN LOẠI SẴN SÀNG (MODULAR)")
        logging.info(f"    GPIO Mode: {'REAL' if isinstance(system.gpio, RealGPIO) else 'MOCK'}")
        logging.info(f"    API State: http://<IP_CUA_PI>:3000")
        if AUTH_ENABLED:
            logging.info(f"    Truy cập: http://<IP_CUA_PI>:3000 (User: {USERNAME} / Pass: {PASSWORD})")
        else:
            logging.info("    Truy cập: http://<IP_CUA_PI>:3000 (KHÔNG yêu cầu đăng nhập)")
        logging.info("=========================================")
        
        # 2. Khởi động Flask web server (chạy ở luồng chính)
        app.run(host='0.0.0.0', port=3000, debug=False, use_reloader=False)

    except KeyboardInterrupt:
        logging.info("\n[MAIN] Dừng hệ thống (Ctrl+C)...")
    except Exception as main_e:
        logging.critical(f"[CRITICAL] Lỗi khởi động hệ thống: {main_e}", exc_info=True)
    finally:
        # 3. Dọn dẹp
        system.stop()