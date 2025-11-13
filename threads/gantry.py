# pi/threads/gantry.py
import time
import logging
import uuid
from core.utils import canon_id # Cần import canon_id

def start_gantry_trigger_thread(system):
    """Luồng tạo Job V2 (Gantry) (Lấy từ app_god.py)"""
    
    sensor_pin_to_read = SENSOR_ENTRY_PIN
    if isinstance(system.gpio, MockGPIO):
        sensor_pin_to_read = SENSOR_ENTRY_MOCK_PIN
        
    logging.info(f"[GANTRY] Thread Gantry Trigger (v2 Logic) (Pin: {sensor_pin_to_read}) bắt đầu.")
    is_first_loop = True

    while system.main_loop_running:
        if system.auto_test_enabled or system.error_manager.is_maintenance():
            time.sleep(0.1); continue
        
        ai_cfg = {}; debounce_time = 0.1; stop_conveyor_enabled = False
        conveyor_stop_delay = 1.0; stability_delay = 0.25 

        with system.state_lock:
            cfg_timing = system.system_state['timing_config']
            debounce_time = cfg_timing.get('sensor_debounce', 0.1) 
            stability_delay = cfg_timing.get('stability_delay', stability_delay) 
            stop_conveyor_enabled = cfg_timing.get('stop_conveyor_on_entry', False)
            conveyor_stop_delay = cfg_timing.get('conveyor_stop_delay', 1.0)
            ai_cfg = system.system_state.get('ai_config', {})

        ai_is_on = ai_cfg.get('enable_ai', False) and system.ai_detector and system.ai_detector.enabled
        ai_has_priority = ai_cfg.get('ai_priority', False)
        now = time.time()

        try:
            sensor_now = system.gpio.input(sensor_pin_to_read)
        except Exception as gpio_e:
            logging.error(f"[GANTRY] Lỗi đọc GPIO pin {sensor_pin_to_read} (SENSOR_ENTRY): {gpio_e}")
            system.error_manager.trigger_maintenance(f"Lỗi đọc sensor ENTRY pin {sensor_pin_to_read}: {gpio_e}")
            time.sleep(0.5); continue

        with system.state_lock:
            system.system_state["sensor_entry_reading"] = sensor_now
        
        if is_first_loop:
            system.last_entry_sensor_state = sensor_now
            is_first_loop = False
            logging.info(f"[GANTRY] Đã 'priming' sensor gác cổng, trạng thái ban đầu: {'ACTIVE' if sensor_now == 0 else 'INACTIVE'}")
            time.sleep(0.1); continue

        if sensor_now == 0 and system.last_entry_sensor_state == 1: # Cạnh xuống (Kích hoạt)
            if (now - system.last_entry_sensor_trigger_time) > debounce_time:
                
                if stability_delay > 0:
                    time.sleep(stability_delay) 
                    if system.gpio.input(sensor_pin_to_read) != 0: 
                        logging.info(f"[GANTRY] Bỏ qua nhiễu tạm thời (dưới {stability_delay}s)")
                        system.last_entry_sensor_state = 1 
                        continue 
                
                system.last_entry_sensor_trigger_time = now
                
                job_lane_index = system.NG_LANE_INDEX
                job_lane_name = system.NG_LANE_NAME
                job_status = "PENDING"; job_track_id = None

                qr_lane_index = None
                try:
                    with system.qr_queue_lock:
                        qr_lane_index = system.qr_queue.pop(0)
                except IndexError: pass

                ai_lane_index = system.NG_LANE_INDEX
                ai_class_name = None; ai_track_id = None
                if ai_is_on:
                    ai_lane_index, ai_class_name, ai_track_id = system.run_ai_detection(system.NG_LANE_INDEX)

                if ai_has_priority and ai_is_on:
                    if ai_lane_index != system.NG_LANE_INDEX:
                        job_lane_index = ai_lane_index; job_status = f"AI_MATCHED ({ai_class_name})"; job_track_id = ai_track_id
                    elif qr_lane_index is not None:
                        job_lane_index = qr_lane_index; job_status = "QR_MATCHED (AI_Fallback)"
                    else: job_status = "ALL_FAILED"
                else:
                    if qr_lane_index is not None:
                        job_lane_index = qr_lane_index; job_status = "QR_MATCHED"
                    elif ai_is_on and ai_lane_index != system.NG_LANE_INDEX:
                        job_lane_index = ai_lane_index; job_status = f"AI_MATCHED ({ai_class_name}) (QR_Fallback)"; job_track_id = ai_track_id
                    else: job_status = "ALL_FAILED"
                
                job_id = str(uuid.uuid4())[:8]; job_id_log_prefix = f"[JobID {job_id}]"
                job = {
                    "job_id": job_id, "lane_index": job_lane_index,
                    "status": job_status, "entry_time": now, "track_id": job_track_id
                }

                if job_lane_index != system.NG_LANE_INDEX:
                    with system.state_lock:
                        if 0 <= job_lane_index < len(system.system_state["lanes"]):
                            job_lane_name = system.system_state["lanes"][job_lane_index]["name"]
                            system.system_state["lanes"][job_lane_index]["status"] = "Đang chờ vật..."
                else: job_lane_name = system.NG_LANE_NAME
                
                current_queue_indices = []
                with system.processing_queue_lock:
                    system.processing_queue.append(job)
                    if len(system.processing_queue) == 1:
                        system.queue_head_since = now
                    current_queue_len = len(system.processing_queue)
                    current_queue_indices = [j["lane_index"] for j in system.processing_queue]
                
                with system.state_lock:
                    system.system_state["queue_indices"] = current_queue_indices
                    system.system_state["entry_queue_size"] = current_queue_len
                
                system.broadcast_log("info", f"{job_id_log_prefix} Vật vào Gác Cổng. Ghép cặp: {job_status} -> Lane '{job_lane_name}' (Track ID: {job_track_id if job_track_id else 'N/A'}).", data={"queue": current_queue_indices})
                logging.info(f"[GANTRY] {job_id_log_prefix} SENSOR_ENTRY kích hoạt. Ghép cặp: {job_status} -> Lane '{job_lane_name}'. Queue chính: {current_queue_len}")

                if stop_conveyor_enabled and job_status == "ALL_FAILED":
                    logging.warning(f"[GANTRY] {job_id_log_prefix} Đọc QR và AI đều thất bại, DỪNG băng chuyền...")
                    system.CONVEYOR_STOP()
                    system.executor.submit(system.restart_conveyor_after_delay, conveyor_stop_delay)

        system.last_entry_sensor_state = sensor_now
        time.sleep(0.05)