# pi/threads/qr_scanner.py
import cv2
import time
import logging
from core.utils import canon_id
from core.qr import PYZBAR, scan_qr_from_frame

def start_qr_scanner_thread(system):
    """Luồng quét QR (V2) (Lấy từ app_god.py)"""
    
    last_qr, last_time = "", 0.0
    
    if PYZBAR: logging.info("[QR_SCAN] Thread QR Scanner (v2 Logic) started (Ưu tiên Pyzbar).")
    else: logging.info("[QR_SCAN] Thread QR Scanner (v2 Logic) started (Chỉ dùng CV2).")

    while system.main_loop_running:
        try:
            if system.auto_test_enabled or system.error_manager.is_maintenance():
                time.sleep(0.2); continue
            
            LANE_MAP = {}
            with system.state_lock:
                LANE_MAP = {canon_id(lane.get("id")): idx 
                            for idx, lane in enumerate(system.system_state["lanes"]) if lane.get("id")}
            
            if not LANE_MAP: time.sleep(0.5); continue

            frame_copy = None
            with system.frame_lock:
                if system.latest_frame is not None: frame_copy = system.latest_frame.copy()
            if frame_copy is None: time.sleep(0.1); continue

            data, qr_source = scan_qr_from_frame(frame_copy)
            
            if data and (data != last_qr or time.time() - last_time > 3.0):
                last_qr, last_time = data, time.time()
                data_key = canon_id(data); data_raw = data

                if data_key in LANE_MAP:
                    idx = LANE_MAP[data_key]
                    current_queue_for_log = []
                    
                    with system.qr_queue_lock: 
                        system.qr_queue.append(idx) 
                        current_queue_for_log = list(system.qr_queue) 
                    
                    system.broadcast_log("qr", f"Phát hiện {system.system_state['lanes'][idx]['name']}", 
                        data={"data_raw": data_raw, "data_key": data_key, "source": qr_source, "queue": current_queue_for_log})
                    logging.info(f"[QR_SCAN] ({qr_source}) Hợp lệ: canon='{data_key}' -> lane {idx}. (Hàng chờ QR Tạm size={len(current_queue_for_log)})")
                            
                elif data_key == "NG":
                    system.broadcast_log("qr_ng", f"Mã NG: {data_raw}", data=data_raw)
                else:
                    system.broadcast_log("unknown_qr", f"Không rõ: {data_key}",
                        data={"data_raw": data_raw, "data_key": data_key, "source": qr_source}) 
                    logging.warning(f"[QR_SCAN] ({qr_source}) Không rõ mã QR: raw='{data_raw}', canon='{data_key}'")
            
            time.sleep(0.1) 

        except Exception as e:
            logging.error(f"[QR_SCAN] Lỗi trong luồng QR Scanner: {e}", exc_info=True)
            time.sleep(0.5)