# pi/threads/camera_trigger.py
import cv2
import time
import logging
import uuid
from core.utils import canon_id
from core.qr import PYZBAR, scan_qr_from_frame # Cần PYZBAR và hàm scan

def start_camera_trigger_thread(system):
    """Luồng tạo Job V1 (Camera) (Lấy từ app_god.py)"""
    
    last_qr, last_time = None, 0.0
    
    if PYZBAR: logging.info("[CAM_TRIG] Thread Camera Trigger (v1 Logic) started (Ưu tiên Pyzbar).")
    else: logging.info("[CAM_TRIG] Thread Camera Trigger (v1 Logic) started (Chỉ dùng CV2).")
        
    NG_LANE_INDEX = system.NG_LANE_INDEX
    NG_LANE_NAME = system.NG_LANE_NAME

    while system.main_loop_running:
        try:
            if system.auto_test_enabled or system.error_manager.is_maintenance():
                time.sleep(0.2); continue
            
            LANE_MAP = {}
            stop_on_qr = False; stop_delay_qr = 2.0
            ai_cfg = {}; qr_debounce_time = 3.0
            
            with system.state_lock:
                LANE_MAP = {canon_id(lane.get("id")): idx 
                            for idx, lane in enumerate(system.system_state["lanes"]) if lane.get("id")}
                cfg_timing = system.system_state.get('timing_config', {})
                stop_on_qr = cfg_timing.get('stop_conveyor_on_qr', False)
                stop_delay_qr = cfg_timing.get('conveyor_stop_delay_qr', 2.0)
                qr_debounce_time = cfg_timing.get('qr_debounce_time', 3.0)
                if qr_debounce_time < 1.0: qr_debounce_time = 1.0
                ai_cfg = system.system_state.get('ai_config', {})

            if not LANE_MAP: time.sleep(0.5); continue

            frame_copy = None
            with system.frame_lock:
                if system.latest_frame is not None: frame_copy = system.latest_frame.copy()
            if frame_copy is None: time.sleep(0.1); continue

            # Sử dụng hàm scan_qr_from_frame đã module hóa
            data, qr_source = scan_qr_from_frame(frame_copy)
            
            now = time.time()
            if data:
                if data != last_qr:
                    last_qr, last_time = data, now
                    data_key = canon_id(data); data_raw = data
                    
                    logging.info(f"[CAM_TRIG] ({qr_source}) Phát hiện mã MỚI: {data_raw}")

                    if data_key in LANE_MAP:
                        ai_is_on = ai_cfg.get('enable_ai', False) and system.ai_detector and system.ai_detector.enabled
                        ai_has_priority = ai_cfg.get('ai_priority', False)
                        
                        job_lane_index = NG_LANE_INDEX; job_lane_name = NG_LANE_NAME
                        job_status = "PENDING"; job_track_id = None
                        
                        qr_lane_index = LANE_MAP[data_key]
                        
                        ai_lane_index = NG_LANE_INDEX
                        ai_class_name = None; ai_track_id = None
                        if ai_is_on:
                            ai_lane_index, ai_class_name, ai_track_id = system.run_ai_detection(NG_LANE_INDEX)

                        if ai_has_priority and ai_is_on:
                            if ai_lane_index != NG_LANE_INDEX:
                                job_lane_index = ai_lane_index; job_status = f"AI_MATCHED ({ai_class_name})"; job_track_id = ai_track_id
                            elif qr_lane_index is not None:
                                job_lane_index = qr_lane_index; job_status = "QR_MATCHED (AI_Fallback)"
                            else: job_status = "ALL_FAILED"
                        else:
                            if qr_lane_index is not None:
                                job_lane_index = qr_lane_index; job_status = "QR_MATCHED"
                            elif ai_is_on and ai_lane_index != NG_LANE_INDEX:
                                job_lane_index = ai_lane_index; job_status = f"AI_MATCHED ({ai_class_name}) (QR_Fallback)"; job_track_id = ai_track_id
                            else: job_status = "ALL_FAILED"
                        
                        job_id = str(uuid.uuid4())[:8]; job_id_log_prefix = f"[JobID {job_id}]"
                        job = {
                            "job_id": job_id, "lane_index": job_lane_index,
                            "status": job_status, "entry_time": now, "track_id": job_track_id
                        }

                        if job_lane_index != NG_LANE_INDEX:
                            with system.state_lock:
                                if 0 <= job_lane_index < len(system.system_state["lanes"]):
                                    job_lane_name = system.system_state["lanes"][job_lane_index]["name"]
                                    system.system_state["lanes"][job_lane_index]["status"] = "Đang chờ vật..."
                        else: job_lane_name = NG_LANE_NAME
                        
                        current_queue_indices = []; current_queue_len = 0
                        with system.processing_queue_lock:
                            system.processing_queue.append(job)
                            if len(system.processing_queue) == 1:
                                system.queue_head_since = now
                            current_queue_len = len(system.processing_queue)
                            current_queue_indices = [j["lane_index"] for j in system.processing_queue]
                        
                        with system.state_lock:
                            system.system_state["queue_indices"] = current_queue_indices
                            system.system_state["entry_queue_size"] = current_queue_len
                        
                        system.broadcast_log("info", f"{job_id_log_prefix} Vật vào Camera (QR). Ghép cặp: {job_status} -> Lane '{job_lane_name}' (Track ID: {job_track_id if job_track_id else 'N/A'}).", data={"queue": current_queue_indices})
                        logging.info(f"[CAM_TRIG] {job_id_log_prefix} Phát hiện QR. Ghép cặp: {job_status} -> Lane '{job_lane_name}'. Queue chính: {current_queue_len}")

                        if stop_on_qr:
                            logging.info(f"[CONVEYOR] {job_id_log_prefix} Phát hiện QR, DỪNG băng chuyền trong {stop_delay_qr}s...")
                            system.CONVEYOR_STOP()
                            system.executor.submit(system.restart_conveyor_after_delay, stop_delay_qr)
                
                elif data == last_qr and (now - last_time) < qr_debounce_time: pass
                else: last_qr = None
            
            time.sleep(0.1) 
        except Exception as e:
            logging.error(f"[CAM_TRIG] Lỗi trong luồng Camera Trigger: {e}", exc_info=True)
            time.sleep(0.5)