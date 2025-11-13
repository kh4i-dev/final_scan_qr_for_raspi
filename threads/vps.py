# pi/threads/vps.py
import requests
import cv2
import time
import logging
import json

def start_vps_thread(system):
    """Gửi dữ liệu lên VPS (Lấy từ app_god.py)"""
    
    # URL và Key này cần được định nghĩa trong config.json
    # Ví dụ: "vps_config": { "url": "https://.../api/pi/update", "api_key": "your-key" }
    vps_url = ""
    api_key = ""
    try:
        with system.state_lock:
            vps_cfg = system.system_state.get('vps_config', {})
            vps_url = vps_cfg.get('url', 'https://phanloai.kh4idev.id.vn/api/pi/update')
            api_key = vps_cfg.get('api_key', 'your-very-secret-key-12345')
            
        if not vps_url or not api_key:
            logging.warning("[VPS_UPDATE] Thiếu 'url' hoặc 'api_key' trong 'vps_config'. Tắt luồng VPS.")
            return
    except Exception as e:
        logging.error(f"[VPS_UPDATE] Lỗi đọc config: {e}. Tắt luồng.")
        return

    time.sleep(5) # Chờ khởi động
    logging.info(f"[VPS_UPDATE] Bắt đầu luồng gửi dữ liệu (Color 20FPS) lên {vps_url}")
    
    session = requests.Session()
    headers = {'X-API-Key': api_key}
    
    while system.main_loop_running:
        try:
            state_copy = None; frame_copy = None
            
            with system.state_lock:
                state_copy = system.get_full_state() # Dùng hàm lấy state
            with system.frame_lock:
                if system.latest_frame is not None:
                    frame_copy = system.latest_frame.copy()
            
            if state_copy is None or frame_copy is None:
                time.sleep(1); continue

            state_json = json.dumps(state_copy)
            ret, buffer = cv2.imencode('.jpg', frame_copy, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if not ret:
                time.sleep(0.5); continue
            
            frame_bytes = buffer.tobytes()
            files = {'frame': ('frame.jpg', frame_bytes, 'image/jpeg')}
            data = {'state': state_json}
            
            response = session.post(vps_url, files=files, data=data, headers=headers, timeout=2.0)
            
            if response.status_code != 200:
                logging.warning(f"[VPS_UPDATE] VPS báo lỗi: {response.status_code} - {response.text}")

            time.sleep(0.05) # ~20 FPS
            
        except requests.exceptions.RequestException as e:
            logging.error(f"[VPS_UPDATE] Lỗi kết nối VPS: {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"[VPS_UPDATE] Lỗi trong luồng: {e}")
            time.sleep(1)